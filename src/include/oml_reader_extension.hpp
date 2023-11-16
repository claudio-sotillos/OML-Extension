#pragma once
#include "duckdb.hpp"

namespace duckdb {

class OmlReaderExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};


struct GlobalState : public GlobalTableFunctionState {
public:
	idx_t rows_read;
};

struct BindState : public TableFunctionData {
public:
	std::vector<std::string> fileRows;
};

} // namespace duckdb
