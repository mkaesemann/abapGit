REPORT zaog_activate_tables.

DATA lt_objects TYPE STANDARD TABLE OF dwinactiv.
DATA ls_obj TYPE dwinactiv.

ls_obj-object = 'TABL'.
ls_obj-obj_name = 'ZAOG_OBJ_STORE'.
APPEND ls_obj TO lt_objects.
ls_obj-obj_name = 'ZAOG_REPO_STATE'.
APPEND ls_obj TO lt_objects.
ls_obj-obj_name = 'ZAOG_PACK_META'.
APPEND ls_obj TO lt_objects.
ls_obj-obj_name = 'ZAOG_PACK_IDX'.
APPEND ls_obj TO lt_objects.
ls_obj-obj_name = 'ZAOG_FETCH_SESS'.
APPEND ls_obj TO lt_objects.
ls_obj-obj_name = 'ZAOG_RAW_PACK'.
APPEND ls_obj TO lt_objects.

CALL FUNCTION 'RS_WORKING_OBJECTS_ACTIVATE'
  EXPORTING
    activate_ddic_objects = 'X'
  TABLES
    objects = lt_objects
  EXCEPTIONS
    OTHERS = 1.

WRITE: / 'Activate RC:', sy-subrc.
