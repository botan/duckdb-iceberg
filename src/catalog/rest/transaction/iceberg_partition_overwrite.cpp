#include "catalog/rest/transaction/iceberg_partition_overwrite.hpp"

#include "catalog/rest/api/iceberg_manifest_merge.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"

#include <cmath>
#include <cstring>

namespace duckdb {

static Value NormalizePartitionValue(const Value &value, const LogicalType &type) {
	auto typed_value = value.DefaultCastAs(type);
	//! Iceberg compares floating partitions by bits, distinguishing signed zero and normalizing NaNs.
	if (type.id() == LogicalTypeId::FLOAT) {
		if (typed_value.IsNull()) {
			return Value(LogicalType::UINTEGER);
		}
		auto number = typed_value.GetValue<float>();
		uint32_t bits;
		std::memcpy(&bits, &number, sizeof(bits));
		return Value::UINTEGER(std::isnan(number) ? 0x7fc00000U : bits);
	}
	if (type.id() == LogicalTypeId::DOUBLE) {
		if (typed_value.IsNull()) {
			return Value(LogicalType::UBIGINT);
		}
		auto number = typed_value.GetValue<double>();
		uint64_t bits;
		std::memcpy(&bits, &number, sizeof(bits));
		return Value::UBIGINT(std::isnan(number) ? 0x7ff8000000000000ULL : bits);
	}
	return typed_value;
}

IcebergPartitionOverwrite::IcebergPartitionOverwrite(const IcebergTableMetadata &metadata,
                                                     const vector<IcebergManifestEntry> &new_files)
    : spec_id(metadata.default_spec_id), schema_id(metadata.GetCurrentSchemaId()) {
	auto &schema = metadata.GetSchemaFromId(schema_id);
	auto &spec = metadata.GetLatestPartitionSpec();
	for (auto &field : spec.fields) {
		if (field.transform == IcebergTransformType::VOID) {
			continue;
		}
		fields.emplace_back(field.partition_field_id,
		                    field.transform.GetSerializedType(schema.GetColumnTypeFromFieldId(field.source_id)));
	}
	for (auto &entry : new_files) {
		D_ASSERT(entry.data_file.record_count > 0);
		partitions.insert(PartitionKey(entry.data_file));
	}
}

Value IcebergPartitionOverwrite::PartitionKey(const IcebergDataFile &file) const {
	if (fields.empty()) {
		return Value::INTEGER(0);
	}
	child_list_t<Value> values;
	for (const auto &[field_id, type] : fields) {
		optional_ptr<const Value> value;
		for (auto &partition : file.partition_info) {
			if (partition.field_id == field_id) {
				value = &partition.value;
				break;
			}
		}
		if (!value) {
			throw InvalidConfigurationException("Partition overwrite: file '%s' is missing partition field %llu",
			                                    file.file_path, field_id);
		}
		values.emplace_back(std::to_string(field_id), NormalizePartitionValue(*value, type));
	}
	return Value::STRUCT(std::move(values));
}

IcebergPartitionOverwrite::file_set_t IcebergPartitionOverwrite::CollectFiles(
    const vector<IcebergManifestListEntry> &manifests, IcebergCommitState &commit_state,
    optional_ptr<const IcebergManifestDeletes> invalidated, optional_ptr<IcebergManifestDeletes> deletes) const {
	file_set_t result;
	auto &metadata = commit_state.table_info.table_metadata;
	for (auto &manifest : manifests) {
		auto loaded = manifest.HasManifestEntries()
		                  ? manifest
		                  : IcebergManifestMerge::ScanManifestEntries(manifest, commit_state, schema_id);
		auto &spec = metadata.partition_specs.at(manifest.file.partition_spec_id);
		for (auto &entry : loaded.GetManifestEntries()) {
			auto &file = entry.data_file;
			IcebergFileIdentity identity(file.file_path, file.content_offset);
			if (entry.status == IcebergManifestEntryStatusType::DELETED ||
			    (invalidated && invalidated->IsInvalidated(identity))) {
				continue;
			}
			const bool is_data = file.content == IcebergManifestEntryContentType::DATA;
			if (is_data && manifest.file.partition_spec_id != spec_id) {
				throw NotImplementedException("Partition overwrite: live data uses partition spec %d, but the current "
				                              "spec is %d; mixed partition specs are not supported",
				                              manifest.file.partition_spec_id, spec_id);
			}
			//! Unpartitioned delete files may apply across partitions. Retain them when replacing a subset,
			//! and conservatively treat changes to them as conflicts. Partitioned deletes apply within their spec.
			const bool global_delete = !is_data && spec.IsUnpartitioned();
			if (!global_delete &&
			    (manifest.file.partition_spec_id != spec_id || !partitions.count(PartitionKey(file)))) {
				continue;
			}
			result.insert(identity);
			if (deletes && (!global_delete || fields.empty())) {
				deletes->InvalidateFile(identity);
			}
		}
	}
	return result;
}

IcebergManifestDeletes IcebergPartitionOverwrite::Initialize(const vector<IcebergManifestListEntry> &manifests,
                                                             const IcebergManifestDeletes &invalidated,
                                                             IcebergCommitState &commit_state) {
	IcebergManifestDeletes deletes;
	initial_files = CollectFiles(manifests, commit_state, &invalidated, &deletes);
	return deletes;
}

void IcebergPartitionOverwrite::Validate(IcebergCommitState &commit_state) const {
	//! REST requirements and retry-state checks guard external schema/spec changes. Later ALTERs in this
	//! transaction may change the defaults while these files still use the captured schema and spec.
	auto current_files = CollectFiles(commit_state.manifests, commit_state, nullptr, nullptr);
	if (current_files != initial_files) {
		throw TransactionException("Partition overwrite on \"%s\" conflicts with a concurrent change to the target "
		                           "partitions; re-run the INSERT",
		                           commit_state.table_info.name);
	}
}

} // namespace duckdb
