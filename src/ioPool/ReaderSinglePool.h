/*
 * (C) Copyright 2022-2023 UCAR
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 */

#pragma once

/*! \defgroup ioda_cxx_io ReaderSinglePool
 * \brief Public API for ioda::ReaderSinglePool
 * \ingroup ioda_cxx_api
 *
 * @{
 * \file IoPoolBase.h
 * \brief Interfaces for ioda::ReaderSinglePool and related classes.
 */

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "eckit/mpi/Comm.h"

#include "ioda/defs.h"
#include "ioda/Engines/ReaderBase.h"
#include "ioda/Engines/ReaderFactory.h"
#include "ioda/Group.h"
#include "ioda/ioPool/IoPoolParameters.h"
#include "ioda/ioPool/ReaderPoolBase.h"

#include "oops/util/DateTime.h"
#include "oops/util/parameters/Parameters.h"
#include "oops/util/parameters/OptionalParameter.h"
#include "oops/util/parameters/RequiredPolymorphicParameter.h"
#include "oops/util/Printable.h"

namespace ioda {
namespace IoPool {

class Distribution;
enum class DateTimeFormat;

/// \brief Reader pool subclass
/// \details This class holds a single io pool which consists of a small number of MPI tasks.
/// The tasks assigned to an io pool object are selected from the total MPI tasks working on
/// the DA run. The tasks in the pool are used to transfer data from a ioda file to memory.
/// Only the tasks in the pool interact with the file and the remaining tasks outside
/// the pool interact with the pool tasks to get their individual pieces of the data being
/// transferred.
/// \ingroup ioda_cxx_io
class ReaderSinglePool : public ReaderPoolBase {
 public:
  /// \brief construct a ReaderSinglePool object
  /// \param configParams Parameters for this io pool
  /// \param createParams Parameters for creating the reader pool
  ReaderSinglePool(const IoPoolParameters & configParams,
                   const ReaderPoolCreationParameters & createParams);
  ~ReaderSinglePool() {}

  /// \brief initialize the io pool after construction
  /// \detail This routine is here to do specialized initialization before the load
  /// function has been called and after the constructor is called.
  void initialize() override;

  /// \brief load obs data from the obs source (file or generator)
  /// \param destGroup destination ioda group to be loaded from the input file
  void load(Group & destGroup) override;

  /// \brief finalize the io pool before destruction
  /// \detail This routine is here to do specialized clean up after the load function has been
  /// called and before the destructor is called. The primary task is to clean up the eckit
  /// split communicator groups.
  void finalize() override;

  /// \brief fill in print routine for the util::Printable base class
  void print(std::ostream & os) const override;

 private:
  /// \brief input file is empty when true
  bool emptyFile_;

  /// \brief string holding YAML description of the file group structure
  /// \details The file group structure is everything in the file except for the variable
  /// data. This includes the hierarchical group structure, the group attributes, the
  /// variables in each group, the dimensions that are attached to each variable, and
  /// the variable attributes.
  std::string groupStructureYaml_;

  /// \brief generate and record the new input file name associated with this rank
  /// \detail The new input file name is set to an empty string for non pool members.
  /// For pool members the new input file name is based on the work directory path
  /// and original file name. For now, an hdf5 file will always be used.
  void setNewInputFileName();

  /// \brief generate and record the file prep info file name
  void setPrepInfoFileName();

  /// \brief restore information related to the prepared files
  /// \param rankGrouping map holding io pool grouping information for the MPI ranks
  /// \details the keys of the grouping map are the ranks in the io pool, and the non
  /// pool member ranks associated with each key are in the values (vector of int,
  /// ie rank numbers).
  /// \param expectedIoPoolRank io pool rank from the prep info file
  void restoreFilePrepInfo(std::map<int, std::vector<int>> & rankGrouping,
                           int & expectedIoPoolRank);

  /// \brief adjust the distribution map according to the new input file
  /// \param fileGroup ioda Group object associated with the file backend
  void adjustDistributionMap(const ioda::Group & fileGroup);

  /// \brief restore the locIndices_ and recNums_ data members
  /// \param fileGroup ioda Group object associated with the file backend
  void restoreIndicesRecNums(const ioda::Group & fileGroup);
};

}  // namespace IoPool
}  // namespace ioda

/// @}
