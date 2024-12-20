/*
 * (C) Copyright 2021 UCAR
 * (C) Crown copyright 2021-2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */
/** @file DataFromSQL.cpp
 * @brief implements ODC bindings
**/

#include <fstream>
#include <algorithm>

#include "ioda/Engines/ODC/OdbConstants.h"
#include "ioda/Engines/ODC/DataFromSQL.h"

#include "odc/Select.h"

#include "oops/util/Duration.h"
#include "oops/util/Logger.h"
#include "oops/util/missingValues.h"

namespace ioda {
namespace Engines {
namespace ODC {

size_t DataFromSQL::getNumberOfRows() const { return number_of_rows_; }

int DataFromSQL::getColumnIndex(const std::string& col) const {
  for (size_t i = 0; i < columns_.size(); i++) {
    if (columns_.at(i) == col) {
      return i;
    }
  }
  return -1;
}

const std::vector<int> &DataFromSQL::getVarnos() const {
  return varnos_;
}

void DataFromSQL::setData(const std::string& sql) {
  odc::Select sodb(sql);
  odc::Select::iterator begin = sodb.begin();
  const size_t number_of_columns = columns_.size();
  ASSERT(begin->columns().size() == number_of_columns);

  // Determine column types and bitfield definitions
  column_types_.clear();
  for (const odc::core::Column *column : begin->columns()) {
    column_types_.push_back(column->type());

    Bitfield bitfield;
    const eckit::sql::BitfieldDef &bitfieldDef = column->bitfieldDef();
    const eckit::sql::FieldNames &fieldNames = bitfieldDef.first;
    const eckit::sql::Sizes &sizes = bitfieldDef.second;
    ASSERT(fieldNames.size() == sizes.size());
    std::int32_t pos = 0;
    for (size_t i = 0; i < fieldNames.size(); ++i) {
      bitfield.push_back({fieldNames[i], pos, sizes[i]});
      pos += sizes[i];
    }
    column_bitfield_defs_.push_back(std::move(bitfield));
  }

  // Retrieve data
  data_.clear();
  data_.resize(number_of_columns);
  for (auto &row : sodb) {
    ASSERT(row.columns().size() == number_of_columns);
    for (size_t i = 0; i < number_of_columns; ++i) {
      data_[i].push_back(static_cast<double>(row[i]));
    }
  }

  // Free unused memory
  for (auto &column : data_) {
    column.shrink_to_fit();
  }
}

int DataFromSQL::getColumnTypeByName(std::string const& column) const {
  return column_types_.at(getColumnIndex(column));
}

bool DataFromSQL::getBitfieldMemberDefinition(const std::string &column, const std::string &member,
                                              int &position, int &size) const {
  const int columnIndex = getColumnIndex(column);
  if (columnIndex < 0)
    return false;
  if (column_types_.at(columnIndex) != odb_type_bitfield)
    return false;

  for (const BitfieldMember& m : column_bitfield_defs_.at(columnIndex))
    if (m.name == member) {
      position = m.start;
      size = m.size;
      return true;
    }

  return false;
}

double DataFromSQL::getData(const size_t row, const size_t column) const {
  if (data_.size() > 0) {
    return data_.at(column).at(row);
  } else {
    return odb_missing_float;
  }
}

double DataFromSQL::getData(const size_t row, const std::string& column) const {
  return getData(row, getColumnIndex(column));
}

const std::vector<std::string>& DataFromSQL::getColumns() const { return columns_; }

void DataFromSQL::select(const std::vector<std::string>& columns, const std::string& filename,
                         const std::vector<int>& varnos, const std::string& query) {
  columns_ = columns;
  std::string sql = "select ";
  for (size_t i = 0; i < columns_.size(); i++) {
    if (i == 0) {
      sql = sql + columns_.at(i);
    } else {
      sql = sql + "," + columns_.at(i);
    }
  }
  sql = sql + " from \"" + filename + "\" where (";
  for (size_t i = 0; i < varnos.size(); i++) {
    if (i == 0) {
      sql = sql + "varno = " + std::to_string(varnos.at(i));
    } else {
      sql = sql + " or varno = " + std::to_string(varnos.at(i));
    }
  }
  sql              = sql + ")";
  if (!query.empty()) {
    sql = sql + " and (" + query + ");";
  } else {
    sql = sql + ";";
  }
  oops::Log::info() << "Using SQL: " << sql << std::endl;
  std::ifstream ifile;
  ifile.open(filename);
  if (ifile) {
    if (ifile.peek() == std::ifstream::traits_type::eof()) {
      ifile.close();
    } else {
      ifile.close();
      setData(sql);
    }
  }
  obsgroup_        = getData(0, getColumnIndex("ops_obsgroup"));
  number_of_rows_  = data_.empty() ? 0 : data_.front().size();
  int varno_column = getColumnIndex("varno");
  if (varno_column >= 0) {
    for (size_t i = 0; i < number_of_rows_; i++) {
      int varno = getData(i, varno_column);
      if (std::find(varnos_.begin(), varnos_.end(), varno) == varnos_.end()) {
        varnos_.push_back(varno);
      }
    }
  }
}

int DataFromSQL::getObsgroup() const { return obsgroup_; }

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
