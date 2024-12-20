#pragma once
/*
 * (C) Copyright 2021 UCAR
 * (C) Crown copyright 2021-2024, Met Office
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#include <string>

namespace ioda {
namespace Engines {
namespace ODC {
  enum OdbColumnType {
    odb_type_ignore   = 0,
    odb_type_int      = 1,
    odb_type_real     = 2,
    odb_type_string   = 3,
    odb_type_bitfield = 4
  };

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
}  // namespace ODC
}  // namespace Engines
}  // namespace ioda
