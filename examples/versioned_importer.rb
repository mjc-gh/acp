# frozen_string_literal: true

# Example consumer importer: event identity prevents duplicate inserts, and the
# source version condition prevents an older replay from replacing newer values.
class VersionedImporter
  # Upsert only a strictly newer version, making replays and takeover races safe.
  # rubocop:disable Metrics/MethodLength
  def self.call(model:, records:)
    return if records.empty?

    quoted_table = model.connection.quote_table_name(model.table_name)
    quoted_version = model.connection.quote_column_name("source_updated_at")
    model.upsert_all(
      records,
      unique_by: :external_id,
      on_duplicate: Arel.sql(<<~SQL.squish)
        source_updated_at = EXCLUDED.source_updated_at,
        value = EXCLUDED.value
        WHERE #{quoted_table}.#{quoted_version} < EXCLUDED.#{quoted_version}
      SQL
    )
  end
  # rubocop:enable Metrics/MethodLength
end
