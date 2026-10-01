# frozen_string_literal: true

require "base64"
require "json"
require "securerandom"
require "redis"
require_relative "../acp"

module Acp
  # Redis-backed leases and committed progress for one application environment
  # and program. Redis scripts use one writable primary and intentionally do not
  # claim Redis Cluster compatibility.
  # Redis coordination keeps its protocol and adapter together to make the Lua
  # script boundaries auditable.
  # rubocop:disable Metrics/ClassLength, Metrics/AbcSize, Metrics/MethodLength, Metrics/ParameterLists
  # rubocop:disable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
  class RedisCoordinator
    Progress = Struct.new(:cursor, :revision, keyword_init: true)
    Lease = Struct.new(:worker_id, :token, keyword_init: true)

    CLAIM_SCRIPT = <<~LUA
      local now = redis.call("TIME")
      local now_ms = (now[1] * 1000) + math.floor(now[2] / 1000)
      local deadline = now_ms + tonumber(ARGV[4])
      local held = redis.call("HGET", KEYS[1], "deadline")
      if held and tonumber(held) > now_ms then return 0 end
      redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now_ms)
      local workers = redis.call("ZRANGE", KEYS[2], 0, tonumber(ARGV[7]) - 1)
      local assigned = nil
      local best_score = -1
      for _, member in ipairs(workers) do
        local capacity = tonumber(redis.call("HGET", ARGV[6] .. member, "capacity") or "1")
        local sample = tonumber(string.sub(redis.sha1hex(ARGV[5] .. "|" .. member), 1, 13), 16) / 4503599627370496
        local score = sample * capacity
        if score > best_score then
          assigned = member
          best_score = score
        end
      end
      if assigned ~= ARGV[2] then return 0 end
      redis.call("HSET", KEYS[1], "worker", ARGV[1], "token", ARGV[3], "deadline", deadline)
      redis.call("PEXPIRE", KEYS[1], ARGV[4])
      return 1
    LUA

    RENEW_SCRIPT = <<~LUA
      local now = redis.call("TIME")
      local now_ms = (now[1] * 1000) + math.floor(now[2] / 1000)
      if redis.call("HGET", KEYS[1], "token") ~= ARGV[1] then return 0 end
      if tonumber(redis.call("HGET", KEYS[1], "deadline") or "0") <= now_ms then return 0 end
      redis.call("HSET", KEYS[1], "deadline", now_ms + tonumber(ARGV[2]))
      redis.call("PEXPIRE", KEYS[1], ARGV[2])
      return 1
    LUA

    RELEASE_SCRIPT = <<~LUA
      if redis.call("HGET", KEYS[1], "token") ~= ARGV[1] then return 0 end
      return redis.call("DEL", KEYS[1])
    LUA

    INITIALIZE_SCRIPT = <<~LUA
      local cursor = redis.call("HGET", KEYS[1], "cursor")
      if cursor then
        return {cursor, redis.call("HGET", KEYS[1], "revision")}
      end
      if redis.call("SISMEMBER", KEYS[2], ARGV[1]) == 1 then return {"MISSING"} end
      redis.call("HSET", KEYS[1], "cursor", ARGV[2], "revision", 0, "poll_id", "")
      redis.call("SADD", KEYS[2], ARGV[1])
      return {ARGV[2], "0"}
    LUA

    WORKER_HEARTBEAT_SCRIPT = <<~LUA
      local now = redis.call("TIME")
      local now_ms = (now[1] * 1000) + math.floor(now[2] / 1000)
      redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", now_ms)
      if not redis.call("ZSCORE", KEYS[2], ARGV[4]) and
          redis.call("ZCARD", KEYS[2]) >= tonumber(ARGV[5]) then
        return "WORKER_LIMIT"
      end
      redis.call("HSET", KEYS[1], "capacity", ARGV[1], "active", ARGV[2])
      redis.call("PEXPIRE", KEYS[1], ARGV[3])
      redis.call("ZADD", KEYS[2], now_ms + tonumber(ARGV[3]), ARGV[4])
      return now_ms
    LUA

    ADVANCE_SCRIPT = <<~LUA
      if redis.call("HGET", KEYS[1], "poll_id") == ARGV[2] then return "APPLIED" end
      local now = redis.call("TIME")
      local now_ms = (now[1] * 1000) + math.floor(now[2] / 1000)
      if redis.call("HGET", KEYS[3], "token") ~= ARGV[4] or
          tonumber(redis.call("HGET", KEYS[3], "deadline") or "0") <= now_ms then
        return "LEASE_LOST"
      end
      local revision = redis.call("HGET", KEYS[1], "revision")
      if not revision then
        if redis.call("SISMEMBER", KEYS[2], ARGV[1]) == 1 then return "MISSING" end
        return "CONFLICT"
      end
      if tonumber(revision) ~= tonumber(ARGV[3]) then return "CONFLICT" end
      local current = redis.call("HGET", KEYS[1], "cursor")
      if ARGV[5] < current then return "REGRESSION" end
      redis.call("HSET", KEYS[1], "cursor", ARGV[5], "revision", tonumber(revision) + 1, "poll_id", ARGV[2])
      return "APPLIED"
    LUA

    def initialize(application:, environment:, program:, redis_url: ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0"),
                   redis_options: {}, connection_factory: nil, worker_id: SecureRandom.uuid,
                   lease_ttl: 60, claim_scan_limit: 256, worker_scan_limit: 256)
      [application, environment, program].each do |part|
        unless part.is_a?(String) && !part.empty?
          raise ConfigurationError, "Redis namespace components must be nonempty strings"
        end
      end
      raise ConfigurationError, "lease_ttl must be at least 3 seconds" unless lease_ttl.is_a?(Numeric) && lease_ttl >= 3
      unless claim_scan_limit.is_a?(Integer) && claim_scan_limit.positive?
        raise ConfigurationError, "claim_scan_limit must be a positive integer"
      end
      unless worker_scan_limit.is_a?(Integer) && worker_scan_limit.positive?
        raise ConfigurationError, "worker_scan_limit must be a positive integer"
      end
      unless worker_id.is_a?(String) && !worker_id.empty?
        raise ConfigurationError, "worker_id must be a nonempty string"
      end

      namespace = Base64.urlsafe_encode64(JSON.generate([application, environment, program]), padding: false)
      @prefix = "acp:v1:#{namespace}"
      @redis_url = redis_url
      @redis_options = redis_options.dup.freeze
      @connection_factory = connection_factory
      @worker_id = worker_id.dup.freeze
      @lease_ttl_ms = (lease_ttl * 1000).to_i
      @claim_scan_limit = claim_scan_limit
      @worker_scan_limit = worker_scan_limit
      @tenant_values = {}
      @claim_offset = 0
    end

    attr_reader :worker_id

    def register_tenants(tenant_ids)
      tenant_ids.each_slice(@claim_scan_limit) do |batch|
        identities = batch.map do |tenant_id|
          identity = canonical_tenant_id(tenant_id)
          @tenant_values[identity] = tenant_id
          identity
        end
        with_client do |redis|
          redis.pipelined do |pipeline|
            identities.each { |identity| pipeline.zadd(key("tenants"), 0, identity) }
          end
        end
      end
      nil
    end

    # Inspect at most claim_scan_limit registered IDs. Workers can call this
    # between cycles to discover work without an unbounded Redis scan.
    def claim_available(limit: 1)
      raise ArgumentError, "limit must be a positive integer" unless limit.is_a?(Integer) && limit.positive?

      ids = with_client { |redis| redis.zrange(key("tenants"), @claim_offset, @claim_offset + @claim_scan_limit - 1) }
      if ids.empty?
        @claim_offset = 0
        ids = with_client { |redis| redis.zrange(key("tenants"), 0, @claim_scan_limit - 1) }
      else
        @claim_offset += @claim_scan_limit
      end
      claimed = []
      ids.each do |identity|
        tenant_id = @tenant_values[identity] || decode_tenant_id(identity)
        @tenant_values[identity] = tenant_id
        lease = acquire(tenant_id)
        claimed << [tenant_id, lease] if lease
        break if claimed.length >= limit
      end
      claimed
    end

    def acquire(tenant_id)
      identity = canonical_tenant_id(tenant_id)
      known_worker = with_client { |redis| redis.zscore(key("workers"), encoded_worker_id) }
      heartbeat_worker(capacity: 1, active: 0) unless known_worker
      token = SecureRandom.hex(24)
      claimed = with_client do |redis|
        redis.eval(
          CLAIM_SCRIPT,
          keys: [lease_key(identity), key("workers")],
          argv: [worker_id, encoded_worker_id, token, @lease_ttl_ms, identity, key("worker:"), @worker_scan_limit]
        )
      end
      Lease.new(worker_id: worker_id, token: token).freeze if claimed == 1
    end

    def renewal_interval(_lease)
      @lease_ttl_ms / 3000.0
    end

    def renew(tenant_id, lease)
      identity = canonical_tenant_id(tenant_id)
      result = with_client do |redis|
        redis.eval(RENEW_SCRIPT, keys: [lease_key(identity)], argv: [lease.token, @lease_ttl_ms])
      end
      result == 1
    end

    def valid?(tenant_id, lease)
      identity = canonical_tenant_id(tenant_id)
      with_client { |redis| redis.hget(lease_key(identity), "token") == lease.token }
    end

    def release(tenant_id, lease)
      identity = canonical_tenant_id(tenant_id)
      with_client do |redis|
        redis.eval(RELEASE_SCRIPT, keys: [lease_key(identity)], argv: [lease.token])
      end
      nil
    end

    def read(tenant_id)
      read_state(tenant_id)&.cursor
    end

    def read_state(tenant_id)
      identity = canonical_tenant_id(tenant_id)
      values = with_client { |redis| redis.hgetall(progress_key(identity)) }
      if values.key?("cursor")
        return Progress.new(
          cursor: Timestamp.load(values.fetch("cursor")),
          revision: Integer(values.fetch("revision"))
        ).freeze
      end
      return nil unless with_client { |redis| redis.sismember(key("initialized"), identity) }

      raise MissingProgressError, "committed progress disappeared for tenant #{tenant_id.inspect}"
    end

    def initialize_cursor(tenant_id, cursor)
      initialize_state(tenant_id, cursor)&.cursor
    end

    def initialize_state(tenant_id, cursor)
      identity = canonical_tenant_id(tenant_id)
      timestamp = Timestamp.dump(cursor)
      result = with_client do |redis|
        redis.eval(INITIALIZE_SCRIPT, keys: [progress_key(identity), key("initialized")], argv: [identity, timestamp])
      end
      if result.first == "MISSING"
        raise MissingProgressError, "committed progress disappeared for tenant #{tenant_id.inspect}"
      end

      Progress.new(cursor: Timestamp.load(result[0]), revision: Integer(result[1])).freeze
    end

    def acknowledge(tenant_id, poll_id, cursor)
      state = read_state(tenant_id)
      lease = acquire(tenant_id)
      raise LeaseLostError, "cannot acknowledge progress without ownership" unless lease

      advance(tenant_id, poll_id, cursor, expected_revision: state.revision, lease: lease)
    ensure
      release(tenant_id, lease) if lease
    end

    def advance(tenant_id, poll_id, cursor, expected_revision:, lease:)
      identity = canonical_tenant_id(tenant_id)
      result = with_client do |redis|
        redis.eval(
          ADVANCE_SCRIPT,
          keys: [progress_key(identity), key("initialized"), lease_key(identity)],
          argv: [identity, poll_id, expected_revision, lease.token, Timestamp.dump(cursor)]
        )
      end
      return true if result == "APPLIED"
      raise LeaseLostError, "ownership lease was lost for tenant #{tenant_id.inspect}" if result == "LEASE_LOST"
      if result == "MISSING"
        raise MissingProgressError, "committed progress disappeared for tenant #{tenant_id.inspect}"
      end
      raise CursorRegressionError, "next cursor cannot move backwards" if result == "REGRESSION"
      raise ProgressConflictError, "progress revision changed for tenant #{tenant_id.inspect}" if result == "CONFLICT"

      raise "unexpected Redis progress response: #{result.inspect}"
    end

    def heartbeat_worker(capacity:, active:)
      with_client do |redis|
        result = redis.eval(
          WORKER_HEARTBEAT_SCRIPT,
          keys: [worker_key, key("workers")],
          argv: [capacity, active, @lease_ttl_ms, encoded_worker_id, @worker_scan_limit]
        )
        raise ConfigurationError, "Redis worker_scan_limit exceeded" if result == "WORKER_LIMIT"
      end
    end

    def worker_statuses
      encoded_ids = with_client { |redis| redis.zrange(key("workers"), 0, @worker_scan_limit - 1) }
      encoded_ids.to_h do |encoded_id|
        status = with_client { |redis| redis.hgetall(key("worker:#{encoded_id}")) }
        [decode_worker_id(encoded_id), status.transform_values(&:to_i).freeze]
      end.freeze
    end

    def canonical_tenant_id(tenant_id)
      value = case tenant_id
              when String
                raise ArgumentError, "tenant_id cannot be empty" if tenant_id.empty?

                ["string", tenant_id]
              when Integer then ["integer", tenant_id.to_s]
              else raise ArgumentError, "tenant_id must be a String or Integer"
              end
      Base64.urlsafe_encode64(JSON.generate(value), padding: false)
    end

    private

    def decode_tenant_id(identity)
      type, value = JSON.parse(Base64.urlsafe_decode64(identity + ("=" * ((4 - identity.length % 4) % 4))))
      return value if type == "string"
      return Integer(value) if type == "integer"

      raise ArgumentError, "unknown registered tenant ID type: #{type.inspect}"
    end

    def encoded_worker_id
      Base64.urlsafe_encode64(worker_id, padding: false)
    end

    def decode_worker_id(identity)
      Base64.urlsafe_decode64(identity + ("=" * ((4 - identity.length % 4) % 4)))
    end

    def worker_key
      key("worker:#{encoded_worker_id}")
    end

    def key(name)
      "#{@prefix}:#{name}"
    end

    def lease_key(identity)
      key("lease:#{identity}")
    end

    def progress_key(identity)
      key("progress:#{identity}")
    end

    def with_client
      client = @connection_factory ? @connection_factory.call : Redis.new(url: @redis_url, **@redis_options)
      yield client
    ensure
      client&.close if client.respond_to?(:close)
    end
  end
  # rubocop:enable Metrics/ClassLength, Metrics/AbcSize, Metrics/MethodLength, Metrics/ParameterLists
  # rubocop:enable Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity
end
