/*
 * (C) Copyright 2020-2021 UCAR
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */
/*! \addtogroup ioda_cxx_engines_pub_HH
 *
 * @{
 * \file HH.cpp
 * \brief HDF5 engine interface to the rest of ioda.
 */

#include "ioda/Engines/HH.h"

#include <mutex>
#include <random>
#include <sstream>

#include "./HH/HH-attributes.h"
#include "./HH/HH-groups.h"
#include "./HH/Handles.h"
#include "ioda/Group.h"
#include "ioda/defs.h"

namespace ioda {
namespace Engines {
namespace HH {
unsigned int random_char() {
  // Adapted from https://lowrey.me/guid-generation-in-c-11/
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 255);
  return dis(gen);
}

std::string generate_hex(const unsigned int len) {
  std::stringstream ss;
  for (unsigned i = 0; i < len; i++) {
    const auto rc = random_char();
    std::stringstream hexstream;
    hexstream << std::hex << rc;
    auto hex = hexstream.str();
    ss << (hex.length() < 2 ? '0' + hex : hex);
  }
  return ss.str();
}

std::string genUniqueName() {
  // GUIDs look like: {CD1A91C6-9C1B-454E-AD1C-977F4C72A01C}.
  // We use these for the file name because they are quite unique.
  // HDF5 needs unique names, otherwise it might open the same memory file twice.

  static std::mutex m;

  std::lock_guard<std::mutex> l{m};

  std::array<std::string, 5> uuid
    = {generate_hex(8), generate_hex(4), generate_hex(4), generate_hex(4), generate_hex(12)};
  std::string res
    = uuid[0] + "-" + uuid[1] + "-" + uuid[2] + "-" + uuid[3] + "-" + uuid[4] + ".hdf5";
  return res;
}

const std::map<HDF5_Version, H5F_libver_t> map_h5ver
  = {{HDF5_Version::Earliest, H5F_LIBVER_EARLIEST},
     {HDF5_Version::V18, H5F_LIBVER_V18},
#if H5_VERSION_GE(1, 10, 0)
     {HDF5_Version::V110, H5F_LIBVER_V110},
#endif
#if H5_VERSION_GE(1, 12, 0)
     {HDF5_Version::V112, H5F_LIBVER_V112},
#endif
     {HDF5_Version::Latest, H5F_LIBVER_LATEST}};

HDF5_Version_Range defaultVersionRange() {
#if H5_VERSION_GE(1, 10, 0)
  return HDF5_Version_Range{HDF5_Version::V110, HDF5_Version::Latest};
#else
  // Old HDF5 version fallthrough
  return HDF5_Version_Range{HDF5_Version::V18, HDF5_Version::Latest};
#endif
}

Group createMemoryFile(const std::string& filename, BackendCreateModes mode, bool flush_on_close,
                       size_t increment_len, HDF5_Version_Range compat) {
  using namespace ioda::detail::Engines::HH;

  static const std::map<BackendCreateModes, unsigned int> m{
    {BackendCreateModes::Truncate_If_Exists, H5F_ACC_TRUNC},
    {BackendCreateModes::Fail_If_Exists, H5F_ACC_CREAT}};

  hid_t plid = H5Pcreate(H5P_FILE_ACCESS);
  Expects(plid >= 0);
  HH_hid_t pl(plid, Handles::Closers::CloseHDF5PropertyList::CloseP);

  if (0 > H5Pset_fapl_core(pl.get(), increment_len, flush_on_close))
    throw;  // jedi_throw.add("Reason", "H5Pset_fapl_core failed");
  // H5F_LIBVER_V18, H5F_LIBVER_V110, H5F_LIBVER_V112, H5F_LIBVER_LATEST.
  // Note: this propagates to any files flushed to disk.
  if (0 > H5Pset_libver_bounds(pl.get(), map_h5ver.at(compat.first), map_h5ver.at(compat.second)))
    throw;  // jedi_throw.add("Reason", "H5Pset_libver_bounds failed");

  HH_hid_t f = H5Fcreate(filename.c_str(), m.at(mode), H5P_DEFAULT, pl.get());

  auto backend
    = std::make_shared<detail::Engines::HH::HH_Group>(f, getCapabilitiesInMemoryEngine(), f);
  return ::ioda::Group{backend};
}

Group createFile(const std::string& filename, BackendCreateModes mode, HDF5_Version_Range compat) {
  using namespace ioda::detail::Engines::HH;

  static const std::map<BackendCreateModes, unsigned int> m{
    {BackendCreateModes::Truncate_If_Exists, H5F_ACC_TRUNC},
    {BackendCreateModes::Fail_If_Exists, H5F_ACC_CREAT}};

  hid_t plid = H5Pcreate(H5P_FILE_ACCESS);
  Expects(plid >= 0);
  HH_hid_t pl(plid, Handles::Closers::CloseHDF5PropertyList::CloseP);
  // H5F_LIBVER_V18, H5F_LIBVER_V110, H5F_LIBVER_V112, H5F_LIBVER_LATEST.
  // Note: this propagates to any files flushed to disk.
  if (0 > H5Pset_libver_bounds(pl.get(), map_h5ver.at(compat.first), map_h5ver.at(compat.second)))
    throw;  // jedi_throw.add("Reason", "H5Pset_libver_bounds failed");

  HH_hid_t f = H5Fcreate(filename.c_str(), m.at(mode), H5P_DEFAULT, pl.get());

  auto backend = std::make_shared<detail::Engines::HH::HH_Group>(f, getCapabilitiesFileEngine(), f);
  return ::ioda::Group{backend};
}

Group openFile(const std::string& filename, BackendOpenModes mode, HDF5_Version_Range compat) {
  using namespace ioda::detail::Engines::HH;
  static const std::map<BackendOpenModes, unsigned int> m{
    {BackendOpenModes::Read_Only, H5F_ACC_RDONLY}, {BackendOpenModes::Read_Write, H5F_ACC_RDWR}};

  hid_t plid = H5Pcreate(H5P_FILE_ACCESS);
  Expects(plid >= 0);
  HH_hid_t pl(plid, Handles::Closers::CloseHDF5PropertyList::CloseP);
  if (0 > H5Pset_libver_bounds(pl.get(), map_h5ver.at(compat.first), map_h5ver.at(compat.second)))
    throw;  // jedi_throw.add("Reason", "H5Pset_libver_bounds failed");

  HH_hid_t f = H5Fopen(filename.c_str(), m.at(mode), pl.get());

  auto backend = std::make_shared<detail::Engines::HH::HH_Group>(f, getCapabilitiesFileEngine(), f);

  return ::ioda::Group{backend};
}

Group openMemoryFile(const std::string& filename, BackendOpenModes mode, bool flush_on_close,
                     size_t increment_len, HDF5_Version_Range compat) {
  using namespace ioda::detail::Engines::HH;
  static const std::map<BackendOpenModes, unsigned int> m{
    {BackendOpenModes::Read_Only, H5F_ACC_RDONLY}, {BackendOpenModes::Read_Write, H5F_ACC_RDWR}};

  hid_t plid = H5Pcreate(H5P_FILE_ACCESS);
  Expects(plid >= 0);
  HH_hid_t pl(plid, Handles::Closers::CloseHDF5PropertyList::CloseP);

  const auto h5Result = H5Pset_fapl_core(pl.get(), increment_len, flush_on_close);
  if (h5Result < 0) throw;  // jedi_throw.add("Reason", "H5Pset_fapl_core failed");
  if (0 > H5Pset_libver_bounds(pl.get(), map_h5ver.at(compat.first), map_h5ver.at(compat.second)))
    throw;  // jedi_throw.add("Reason", "H5Pset_libver_bounds failed");

  HH_hid_t f = H5Fopen(filename.c_str(), m.at(mode), pl.get());

  auto backend
    = std::make_shared<detail::Engines::HH::HH_Group>(f, getCapabilitiesInMemoryEngine(), f);

  return ::ioda::Group{backend};
}

Capabilities getCapabilitiesFileEngine() {
  static Capabilities caps;
  static bool inited = false;
  if (!inited) {
    caps.canChunk            = Capability_Mask::Supported;
    caps.canCompressWithGZIP = Capability_Mask::Supported;
    caps.MPIaware            = Capability_Mask::Supported;

    bool canSZIP = false;  //::HH::CanUseSZIP<int>();
    caps.canCompressWithSZIP
      = (canSZIP) ? Capability_Mask::Supported : Capability_Mask::Unsupported;
  }

  return caps;
}

Capabilities getCapabilitiesInMemoryEngine() {
  static Capabilities caps;
  static bool inited = false;
  if (!inited) {
    caps.canChunk            = Capability_Mask::Supported;
    caps.canCompressWithGZIP = Capability_Mask::Supported;
    caps.MPIaware            = Capability_Mask::Unsupported;

    bool canSZIP = false;  //::HH::CanUseSZIP<int>();
    caps.canCompressWithSZIP
      = (canSZIP) ? Capability_Mask::Supported : Capability_Mask::Unsupported;
  }

  return caps;
}

}  // namespace HH
}  // namespace Engines
}  // namespace ioda

/// @}
