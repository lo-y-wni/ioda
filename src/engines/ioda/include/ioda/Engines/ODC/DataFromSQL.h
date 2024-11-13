#pragma once
/*
 * (C) Copyright 2021 UCAR
 * (C) Crown copyright 2021-2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */
/** @file DataFromSQL.h
 * @brief implements ODC bindings
**/

#include <cctype>
#include <iomanip>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "ioda/defs.h"
#include "ioda/ObsGroup.h"

#include "oops/util/DateTime.h"
#include "oops/util/missingValues.h"

#include "unsupported/Eigen/CXX11/Tensor"

namespace ioda {
namespace Engines {
namespace ODC {
  static constexpr int odb_type_int      = 1;
  static constexpr int odb_type_real     = 2;
  static constexpr int odb_type_string   = 3;
  static constexpr int odb_type_bitfield = 4;

  static constexpr float odb_missing_float = -2147483648.0f;
  static constexpr int odb_missing_int = 2147483647;
  static constexpr char* odb_missing_string = const_cast<char *>("MISSING*");

  template <class T>
  T odb_missing();
  template <>
  inline float odb_missing<float>() { return odb_missing_float; }
  template <>
  inline int odb_missing<int>() { return odb_missing_int; }
  template <>
  inline std::string odb_missing<std::string>() { return odb_missing_string; }

class DataFromSQL {
private:
  template <typename T>
  using ArrayX = Eigen::Array<T, Eigen::Dynamic, 1>;

  /// Member of a bitfield column
  struct BitfieldMember {
    std::string name;
    std::int32_t start = 0;  // index of the first bit belonging to the member
    std::int32_t size = 1;   // number of bits belonging to the member
  };
  /// All members of a bitfield column
  typedef std::vector<BitfieldMember> Bitfield;

  std::vector<std::string> columns_;
  std::vector<int> column_types_;
  std::vector<Bitfield> column_bitfield_defs_;
  std::vector<int> varnos_;
  /// Each element contains values from a particular column
  std::vector<std::vector<double>> data_;
  size_t number_of_rows_           = 0;
  int obsgroup_                    = 0;
  /// \brief Populate structure with data from an sql
  /// \param sql The SQL string to generate the data for the structure
  void setData(const std::string& sql);

public:
  /// \brief Simple constructor
  DataFromSQL() = default;

  /// \brief Returns the total number of rows.
  size_t getNumberOfRows() const;

  /// \brief Populate structure with data from specified columns, file and varnos
  /// \param columns List of columns to extract
  /// \param filename Extract from this file
  /// \param varnos List of varnos to extract
  /// \param query Selection criteria to apply
  void select(const std::vector<std::string>& columns, const std::string& filename,
              const std::vector<int>& varnos, const std::string& query);

  const std::vector<int> &getVarnos() const;

  /// \brief Returns the index of a specified column
  /// \param column The column to check
  int getColumnIndex(const std::string& column) const;

  /// \brief Returns the value for a particular row/column
  /// \param row Get data for this row
  /// \param column Get data for this column
  double getData(size_t row, size_t column) const;

  /// \brief Returns the value for a particular row/column (as name)
  /// \param row Get data for this row
  /// \param column Get data for this column
  double getData(size_t row, const std::string& column) const;

  /// \brief Returns the vector of names of columns selected by the SQL query
  const std::vector<std::string>& getColumns() const;

  /// \brief Returns the type of a specified column
  /// \param column The column to check
  int getColumnTypeByName(std::string const& column) const;

  /// \brief Get the position and size of a bitfield column member.
  ///
  /// \param[in] column Bitfield column name.
  /// \param[in] member Bitfield member name.
  /// \param[out] positions Index of the first bit belonging to the member.
  /// \param[out] size Number of bits belonging to the member.
  ///
  /// \returns True if \p column exists, is a bitfield column and has a member \p member, false
  /// otherwise.
  bool getBitfieldMemberDefinition(const std::string &column, const std::string &member,
                                   int &position, int &size) const;

  /// \brief Returns the obsgroup number
  int getObsgroup() const;
};

}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
