#pragma once

#include "duckdb/common/types/value_map.hpp"
#include "core/metadata/iceberg_file_identity.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "catalog/rest/transaction/iceberg_transaction_metadata.hpp"

namespace duckdb {

struct IcebergCommitState;

//! The partitions replaced by one INSERT and the live files observed in those partitions.
//! File identities survive manifest merging, so unrelated metadata rewrites do not cause conflicts.
struct IcebergPartitionOverwrite {
	IcebergPartitionOverwrite(const IcebergTableMetadata &metadata, const vector<IcebergManifestEntry> &new_files);

	IcebergManifestDeletes Initialize(const vector<IcebergManifestListEntry> &manifests,
	                                  const IcebergManifestDeletes &invalidated, IcebergCommitState &commit_state);
	void Validate(IcebergCommitState &commit_state) const;

private:
	using file_set_t = unordered_set<IcebergFileIdentity, IcebergFileIdentityHash>;

	Value PartitionKey(const IcebergDataFile &file) const;
	file_set_t CollectFiles(const vector<IcebergManifestListEntry> &manifests, IcebergCommitState &commit_state,
	                        optional_ptr<const IcebergManifestDeletes> invalidated,
	                        optional_ptr<IcebergManifestDeletes> deletes) const;

	int32_t spec_id;
	int32_t schema_id;
	//! Spec order and canonical types are shared by writer strings and typed Avro partition values.
	vector<pair<uint64_t, LogicalType>> fields;
	value_set_t partitions;
	file_set_t initial_files;
};

} // namespace duckdb
