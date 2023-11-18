#define DUCKDB_EXTENSION_MAIN

#include "oml_reader_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include <fstream>
#include <cassert>
#include <iostream>


#include "oml_parser.cpp"
#include "oml_parser.hpp"


namespace duckdb
{   
    ////////////////////////////////////////////
    //      OmlGen  Functions                //
    ///////////////////////////////////////////
    inline void OMLScanInternal(ClientContext &context, TableFunctionInput &data_p, DataChunk &output){
        auto &global_data = data_p.global_state->Cast<GlobalState>();
        auto &bind_data = data_p.bind_data->Cast<BindState>();
        idx_t actual_row = 0;

        for (size_t i = global_data.rows_read; i < bind_data.fileRows.size(); ++i) {
            if (actual_row >= STANDARD_VECTOR_SIZE ){
                break;
            }
            std::istringstream iss(bind_data.fileRows[i]);
            std::vector<std::string> tokens;

            // break the row into a list of numbers
            do {
                std::string token;
                iss >> token;
                if (!token.empty()) {
                    tokens.push_back(token);
                }
            } while (iss);

            
            // Going over the row values (their idx (j) corresponds to the column idx)
            for (size_t j = 0; j < tokens.size(); j++) {
                duckdb::Value duckVal(tokens.at(j));
                output.SetValue(j, actual_row, duckVal);
            }
            actual_row ++; 
            global_data.rows_read++;
        }
        output.SetCardinality(actual_row);  
    }


    // Bind Function for Table creation 
    unique_ptr<FunctionData> OMLScanBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names){
        auto file = input.inputs[0].ToString();
        auto result = make_uniq<BindState>();

        OMLParser parser(return_types, names, file);
        return_types = parser.GetReturnTypes();
        names = parser.GetNames();
        result->fileRows =  parser.GetRows();
        return std::move(result);
    }

    // Setup GLobal State 
    unique_ptr<GlobalTableFunctionState> OMLScanInitGlobal(ClientContext &context, TableFunctionInitInput &input){
        auto result = make_uniq<GlobalState>();
        // auto bind_data = input.bind_data->Cast<BindState>();
        result->rows_read = 0;
        return std::move(result);
    }



    ////////////////////////////////////////////
    //    Power_Consumption_load  Functions  //
    ///////////////////////////////////////////
    
    inline void POWScanInternal(ClientContext &context, TableFunctionInput &data_p, DataChunk &output){
        auto &global_data = data_p.global_state->Cast<GlobalState>();
        auto &bind_data = data_p.bind_data->Cast<BindState>();
        idx_t actual_row = 0;

        // auto &catalog = Catalog::GetCatalog(context, "");
        // auto &tbl_catalog = catalog.GetEntry<TableCatalogEntry>(context, bind_data.schema, bind_data.table);

        for (size_t i = global_data.rows_read; i < bind_data.fileRows.size(); ++i) {
            if (actual_row >= STANDARD_VECTOR_SIZE ){
                break;
            }
            std::istringstream iss(bind_data.fileRows[i]);
            std::vector<std::string> tokens;

            // break the row into a list of numbers
            do {
                std::string token;
                iss >> token;
                if (!token.empty()) {
                    tokens.push_back(token);
                }
            } while (iss);

            
            // Going over the row values (their idx (j) corresponds to the column idx)
            for (size_t j = 0; j < tokens.size(); j++) {
                duckdb::Value duckVal(tokens.at(j));
                output.SetValue(j, actual_row, duckVal);
            }
            actual_row ++; 
            global_data.rows_read++;
        }
        output.SetCardinality(actual_row);  
    }



    unique_ptr<FunctionData> POWScanBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names){
        auto file = input.inputs[0].ToString();
        auto result = make_uniq<BindState>();
        OMLParser parser(return_types, names, file);
        names = parser.GetNames();

        // Check if the file has as many columns as the table we are going to create
        // 
        if (!(names.size() == 8)) {
            std::cerr << "Assertion failed: The file that you are inputing doesn't have 8 columns." << std::endl;
            std::cerr << "Actual Amount of columns: " << names.size() << std::endl;
            // assert(names.size() == 8);
            D_ASSERT(names.size() == 8) ;            
        }
    
        return_types = parser.GetReturnTypes();

        auto info = make_uniq<CreateTableInfo>();
        info->schema = "main";
        info->table =  "Power_Consumption";
        info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
        info->temporary = false;
        names = {"experiment_id", "node_id", "node_id_seq", "time_sec", "time_usec", "power", "current", "voltage"};
        vector<bool> not_null_constraint = {false, false, false, true, true, true, true, true};
        for (idx_t i = 0; i < return_types.size(); i++) {
            info->columns.AddColumn(ColumnDefinition(names[i], return_types[i]));
            if (not_null_constraint.size() != 0 && not_null_constraint[i])
                info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
        }
        auto &catalog = Catalog::GetCatalog(context, "");
        catalog.CreateTable(context, std::move(info));

    
        result->fileRows =  parser.GetRows();
        return std::move(result);
    }


    ////////////////////////////////////////////
    //          Loading All Functions         //
    ///////////////////////////////////////////
    
    static void LoadInternal(DatabaseInstance &instance)
    {   
        // Register all table functions
        //  OMLScanBind -- Used to create the table Schema based on the metadata of the input file. The reading is done by the Oml Parser class.
        //  OMLScanInternal -- Loads the tuples (rows) of the file into the already defined table.
        auto oml_gen= TableFunction("OmlGen", {LogicalType::VARCHAR}, OMLScanInternal ,OMLScanBind,OMLScanInitGlobal);
        ExtensionUtil::RegisterFunction(instance, oml_gen);


        auto oml_POW= TableFunction("Power_Consumption_load", {LogicalType::VARCHAR}, POWScanInternal ,POWScanBind, OMLScanInitGlobal);
        ExtensionUtil::RegisterFunction(instance, oml_POW);
    }

    void OmlReaderExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }
    std::string OmlReaderExtension::Name()
    {
        return "oml_reader";
    }

} // namespace duckdb

extern "C"
{

    DUCKDB_EXTENSION_API void oml_reader_init(duckdb::DatabaseInstance &db)
    {
        LoadInternal(db);
        
    }

    DUCKDB_EXTENSION_API const char *oml_reader_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif