!
! (C) Copyright 2023 UCAR
!
! This software is licensed under the terms of the Apache Licence Version 2.0
! which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
module ioda_f_c_string_mod
   use, intrinsic :: iso_c_binding
   use, intrinsic :: iso_fortran_env

   interface

      function c_alloc(n) result(p) bind(C, name="malloc")
         import c_ptr, c_size_t
         integer(c_size_t), value :: n
         type(c_ptr) :: p
      end function

      subroutine c_free(p) bind(C, name="free")
         import c_ptr
         type(c_ptr), value :: p
      end subroutine

      function c_strlen(p) result(n) bind(C, name="strlen")
         import c_ptr, c_size_t
         type(c_ptr), value :: p
         integer(c_size_t) :: n
      end function

   end interface

contains
   logical function ioda_c_associated(ptr) result(assoc)
      implicit none
      type(C_ptr), intent(in) :: ptr
      assoc = .true.
      if (.not. c_associated(ptr)) then
         assoc = .false.
      end if
   end function

   function ioda_c_strlen(ptr) result(n)
      implicit none
      type(C_ptr), intent(in) :: ptr
      integer(int64) :: n
      if (ioda_c_associated(ptr)) then
         n = c_strlen(ptr)
      else
         write (error_unit, *) 'warning ioda_c_strlen pointer is null'
         n = 0
      end if
   end function

   function ioda_c_string_alloc(n) result(p)
      implicit none
      type(c_ptr) :: p
      integer(int64), intent(in) :: n
      p = c_alloc((n + 1))
   end function

   subroutine ioda_c_free(p)
      implicit none
      type(c_ptr) :: p
      if (ioda_c_associated(p)) then
         call c_free(p)
      end if
      p = c_null_ptr
   end subroutine

   function ioda_f_string_to_c_dup(fstr) result(cstr_ptr)
      implicit none
      character(len=*), intent(in) :: fstr
      type(c_ptr) :: cstr_ptr
      character(kind=c_char, len=1), dimension(:), pointer :: cstr
      integer(int64) :: i, clen, flen

      flen = len_trim(fstr)
      if (len(fstr) .lt. flen) then
         write (error_unit, *) 'major logic error len_trim > len of fortran string'
         stop - 1
      end if
      clen = flen
      cstr_ptr = ioda_c_string_alloc(clen)
      clen = clen + 1
      call c_f_pointer(cstr_ptr, cstr, [clen])
      do i = 1, flen
         cstr(i) = fstr(i:i)
      end do
      cstr(clen) = c_null_char
   end function ioda_f_string_to_c_dup

   subroutine ioda_c_string_print(cstr_ptr, wunit)
      implicit none
      type(c_ptr) :: cstr_ptr
      character(kind=c_char, len=1), dimension(:), pointer :: cstr
      integer, intent(in), optional :: wunit
      integer(int64) :: nlen
      integer :: wr_unit

      if (present(wunit) .eqv. .false.) then
         wr_unit = output_unit
      else
         wr_unit = wunit
      end if
      nlen = ioda_c_strlen(cstr_ptr)
      call c_f_pointer(cstr_ptr, cstr, [nlen])
      write (wr_unit, *) 'cstr = <', cstr(1:nlen),'>'
   end subroutine

   subroutine ioda_f_string_to_c_copy(fstr, cstr_ptr, max_c_sz)
      implicit none
      character(len=*), intent(in) :: fstr
      type(c_ptr), intent(inout) :: cstr_ptr
      integer(int64), intent(inout) :: max_c_sz
      character(kind=c_char, len=1), dimension(:), pointer :: cstr
      integer(int64) :: i, clen, flen

      flen = len_trim(fstr)
      if (max_c_sz < flen) then
         call ioda_c_free(cstr_ptr)
         cstr_ptr = ioda_c_string_alloc(flen)
         max_c_sz = flen
      end if
      clen = flen + 1
      call c_f_pointer(cstr_ptr, cstr, [clen])
      do i = 1, flen
         cstr(i) = fstr(i:i)
      end do
      cstr(clen) = c_null_char
   end subroutine

   subroutine ioda_c_string_to_f_dup(cstr_ptr, fstr)
      implicit none
      type(c_ptr), intent(in) :: cstr_ptr
      character(len=:), allocatable, intent(out) :: fstr
      character(kind=c_char, len=1), dimension(:), pointer :: cstr
      integer(int64) :: i, clen, flen, nlen

      if (.not. ioda_c_associated(cstr_ptr)) then
         write (error_unit, *) 'ioda_c_string_to_f_copy c ptr is null'
         stop - 1
      end if
      if (allocated(fstr)) then
         deallocate (fstr)
      end if
      clen = ioda_c_strlen(cstr_ptr)
      nlen = clen + 1
      allocate (character(len=clen) :: fstr)
      call c_f_pointer(cstr_ptr, cstr, [nlen])
      do i = 1, clen
         fstr(i:i) = cstr(i)
      end do
   end subroutine

   subroutine ioda_c_string_to_f_copy(cstr_ptr, fstr)
      implicit none
      type(c_ptr) :: cstr_ptr
      character(len=*), intent(out) :: fstr
      character(kind=c_char, len=1), dimension(:), pointer :: cstr
      integer(int64) :: i, clen, flen, nlen

      if (.not. c_associated(cstr_ptr)) then
         write (error_unit, *) ' null ptr passed to ioda_c_string_to_f_copy'
         stop - 1
      end if
      flen = len(fstr)
      clen = ioda_c_strlen(cstr_ptr)
      if (flen < clen) then
         write (error_unit, *) 'ioda_c_string_to_f_copy fortran string is too small to hold c string'
         stop - 1
      end if
      nlen = clen + 1
      call c_f_pointer(cstr_ptr, cstr, [nlen])
      do i = 1, clen
         fstr(i:i) = cstr(i)
      end do
      if (flen > clen) then
         fstr(clen + 1:flen) = ' '
      end if
   end subroutine

end module
