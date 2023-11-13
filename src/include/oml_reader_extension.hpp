#pragma once


#include "duckdb.hpp"



namespace duckdb {

class OmlReaderExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};


struct OMLGlobalState : public GlobalTableFunctionState {
public:
	idx_t chunk_count;
	idx_t rows_read;
	vector<LogicalType> types;
};

struct OmlBindState : public TableFunctionData {
public:
	std::string file;
	std::vector<std::string> fileRows;

};

} // namespace duckdb
