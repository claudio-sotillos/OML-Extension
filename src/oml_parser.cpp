#include "oml_parser.hpp"
#include "duckdb.hpp"
#include <vector>
#include <iostream>


namespace duckdb {
    
    class OMLParser {
        private:
            std::vector<duckdb::LogicalType> return_types;
            std::vector<std::string> names;
            std::string file;
            std::vector<std::string> fileRows;
            int row_idx;

        public:
            OMLParser(std::vector<duckdb::LogicalType> return_types, std::vector<std::string> names, std::string file):
            
                return_types(return_types), names(names), file(file) {

                // Check if file exists
                std::ifstream infile(file);
                if (!infile.good()) {
                    throw std::runtime_error("File does not exist.");
                }
                

                // Read the whole file
                for (std::string row_i; std::getline(infile, row_i); ) {
                    this->fileRows.push_back(row_i);
                }
                // Process the header of the OML file
                this-> ReadMetadata();

                // Keep just the rows with the numerical data
                this->fileRows =  this->Slicer(this->fileRows, this-> row_idx, this->fileRows.size()-1);
            }

            // EXTRA UTILITY FUNCTIONS 

            // Slicing function for vectors
            std::vector<std::string> Slicer(std::vector<std::string>& arr, int X, int Y){
                auto start = arr.begin() + X;
                auto end = arr.begin() + Y + 1;
                std::vector<std::string> result(Y - X + 1);
                copy(start, end, result.begin());
                return result;
            }

            // Getter function for return_types
            std::vector<duckdb::LogicalType> GetReturnTypes() const {
                return return_types;
            }

            // Getter function for file
            std::string GetFile() const {
                return file;
            }

            // Getter function for names
            std::vector<std::string> GetNames() const {
                return names;
            }

            // Getter function for stored rows
            std::vector<std::string> GetRows() const {
                return fileRows;
            }


        private:
            void ReadMetadata(){
                int schemaRead = 0;
                size_t counter = 0;

                while(counter < this->fileRows.size()){
        
                    auto row_i = this->fileRows.at(counter);

                    if (row_i.find("schema:") != std::string::npos) {
                        std::istringstream schemaRow(row_i);
                        std::string token;
                        while (schemaRow >> token) {
                            if (token.find(":") != std::string::npos && token != "schema:" ) {
                                size_t pos = token.find(":");

                                this->names.push_back(token.substr(0, pos));
                                    
                                if( token.substr(pos + 1) == "uint32"  || token.substr(pos + 1) == "int32"){
                                    this->return_types.push_back(duckdb::LogicalType::INTEGER);
                                } else if( token.substr(pos + 1) == "string"){
                                    this->return_types.push_back(duckdb::LogicalType::VARCHAR);
                                } else if( token.substr(pos + 1) == "double"){
                                    this->return_types.push_back(duckdb::LogicalType::DOUBLE);
                                } else {
                                    std::cout << "Unknown Type"  << std::endl;
                                }
                            }
                        }
                        schemaRead += 1;                     

                    } else if (schemaRead >= 2 &&  this->SplitBy(row_i, "\t").size() == this->return_types.size()){
                        this->row_idx = counter;
                        break;
                    }
                    counter++;
                }
            }

            // Splitting function to split a string bu a separator
            std::vector<std::string>  SplitBy (const std::string &row_i, std::string separator) {
                std::vector<std::string> res;
                size_t pos = 0;
                size_t next_pos = 0;

                while ((next_pos = row_i.find(separator, pos)) != std::string::npos) {
                    res.push_back(row_i.substr(pos, next_pos - pos));
                    pos = next_pos + separator.size();
                }
                res.push_back(row_i.substr(pos));

                return res;
            }
    };

}
