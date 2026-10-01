# frozen_string_literal: true

require "acp"

# This definition stores callbacks without executing discovery at load time.
class TenantSync < Acp::Program
  interval 60
  fetch_concurrency 20
  ingest_concurrency 4
  pipeline_capacity 24

  tenants do |emit|
    Tenant.active.in_batches do |batch|
      batch.pluck(:id).each { |tenant_id| emit.call(tenant_id) }
    end
  end

  initial_cursor do |tenant_id|
    Tenant.find(tenant_id).sync_started_at
  end

  resolve do |tenant_id|
    tenant = Tenant.find(tenant_id)
    { id: tenant.id, credentials: tenant.api_credentials }
  end

  fetch do |tenant, context|
    response = MyAPIClient.new(tenant[:credentials]).fetch(since: context.cursor)
    Acp::Batch.new(data: response.records, next_cursor: response.next_timestamp)
  end

  ingest do |batch, context|
    MyImporter.call(tenant_id: context.tenant_id, records: batch.data)
  end
end
