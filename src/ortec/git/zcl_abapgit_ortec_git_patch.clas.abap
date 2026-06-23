CLASS zcl_abapgit_ortec_git_patch DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.

    TYPES:
      BEGIN OF ty_subblock_info,
        first_line TYPE i,
        last_line  TYPE i,
        inserted   TYPE i,
        deleted    TYPE i,
      END OF ty_subblock_info,
      ty_subblock_info_tt TYPE STANDARD TABLE OF ty_subblock_info WITH DEFAULT KEY.

    TYPES:
      BEGIN OF ty_hunk_info,
        section_num TYPE i,
        beacon_text TYPE string,
        inserted    TYPE i,
        deleted     TYPE i,
        subblocks   TYPE ty_subblock_info_tt,
      END OF ty_hunk_info,
      ty_hunk_info_tt TYPE STANDARD TABLE OF ty_hunk_info WITH DEFAULT KEY.

    TYPES:
      BEGIN OF ty_file_info,
        file_index   TYPE i,
        filename     TYPE string,
        path         TYPE string,
        nfname       TYPE string,
        obj_type     TYPE string,
        obj_name     TYPE string,
        lstate       TYPE c LENGTH 1,
        rstate       TYPE c LENGTH 1,
        total_insert TYPE i,
        total_delete TYPE i,
        hunks        TYPE ty_hunk_info_tt,
      END OF ty_file_info,
      ty_file_info_tt TYPE STANDARD TABLE OF ty_file_info WITH DEFAULT KEY.

    CLASS-METHODS build_nav_data
      IMPORTING
        it_diff_files  TYPE zif_abapgit_gui_diff=>ty_file_diffs
      RETURNING
        VALUE(rt_data) TYPE ty_file_info_tt.

    CLASS-METHODS render_nav_json
      IMPORTING
        it_diff_files  TYPE zif_abapgit_gui_diff=>ty_file_diffs
      RETURNING
        VALUE(rv_json) TYPE string.

    CLASS-METHODS render_nav_data_script
      IMPORTING
        it_diff_files  TYPE zif_abapgit_gui_diff=>ty_file_diffs
      RETURNING
        VALUE(ri_html) TYPE REF TO zif_abapgit_html.

    CLASS-METHODS render_scripts
      RETURNING
        VALUE(ri_html) TYPE REF TO zif_abapgit_html.

    CLASS-METHODS render_styles
      RETURNING
        VALUE(ri_html) TYPE REF TO zif_abapgit_html.

  PRIVATE SECTION.

    CLASS-METHODS normalize_fname
      IMPORTING
        iv_path           TYPE string
        iv_filename       TYPE string
      RETURNING
        VALUE(rv_nfname)  TYPE string.

    CLASS-METHODS escape_json
      IMPORTING
        iv_val            TYPE string
      RETURNING
        VALUE(rv_escaped) TYPE string.

ENDCLASS.



CLASS zcl_abapgit_ortec_git_patch IMPLEMENTATION.


  METHOD normalize_fname.

    rv_nfname = replace( val  = iv_path
                         sub  = '/'
                         occ  = 0
                         with = '_' )
             && '_'
             && replace( val  = iv_filename
                         sub  = '.'
                         occ  = 0
                         with = '_' ).

  ENDMETHOD.


  METHOD escape_json.

    rv_escaped = iv_val.
    rv_escaped = replace( val  = rv_escaped
                          sub  = '\'
                          occ  = 0
                          with = '\\' ).
    rv_escaped = replace( val  = rv_escaped
                          sub  = '"'
                          occ  = 0
                          with = '\"' ).
    rv_escaped = replace( val  = rv_escaped
                          sub  = cl_abap_char_utilities=>cr_lf
                          occ  = 0
                          with = '\n' ).
    rv_escaped = replace( val  = rv_escaped
                          sub  = cl_abap_char_utilities=>newline
                          occ  = 0
                          with = '\n' ).
    rv_escaped = replace( val  = rv_escaped
                          sub  = '</'
                          occ  = 0
                          with = '<\/' ).

  ENDMETHOD.


  METHOD build_nav_data.

    DATA ls_file     TYPE zif_abapgit_gui_diff=>ty_file_diff.
    DATA ls_info     TYPE ty_file_info.
    DATA ls_hunk     TYPE ty_hunk_info.
    DATA lt_diffs    TYPE zif_abapgit_definitions=>ty_diffs_tt.
    DATA ls_diff     TYPE zif_abapgit_definitions=>ty_diff.
    DATA ls_bcon     TYPE zif_abapgit_definitions=>ty_diff.
    DATA lt_beacons  TYPE zif_abapgit_definitions=>ty_string_tt.
    DATA lv_insert   TYPE abap_bool.
    DATA lv_section  TYPE i.
    DATA lv_tabix    TYPE sy-tabix.
    DATA lv_beacon   TYPE string.
    DATA lv_in_block TYPE abap_bool.
    DATA ls_subblock TYPE ty_subblock_info.

    FIELD-SYMBOLS <ls_hunk> TYPE ty_hunk_info.

    LOOP AT it_diff_files INTO ls_file.

      CLEAR ls_info.
      ls_info-file_index = sy-tabix - 1.
      ls_info-filename   = ls_file-filename.
      ls_info-path       = ls_file-path.
      ls_info-nfname     = normalize_fname( iv_path     = ls_file-path
                                            iv_filename = ls_file-filename ).
      ls_info-obj_type   = ls_file-obj_type.
      ls_info-obj_name   = ls_file-obj_name.
      ls_info-lstate     = ls_file-lstate.
      ls_info-rstate     = ls_file-rstate.

      IF ls_file-o_diff IS NOT BOUND.
        APPEND ls_info TO rt_data.
        CONTINUE.
      ENDIF.

      lt_diffs   = ls_file-o_diff->get( ).
      lt_beacons = ls_file-o_diff->get_beacons( ).
      lv_insert  = abap_true.
      lv_in_block = abap_false.

      LOOP AT lt_diffs INTO ls_diff.
        lv_tabix = sy-tabix.

        IF ls_diff-short = abap_false.
          IF lv_in_block = abap_true.
            READ TABLE ls_info-hunks ASSIGNING <ls_hunk>
              INDEX lines( ls_info-hunks ).
            IF sy-subrc = 0.
              APPEND ls_subblock TO <ls_hunk>-subblocks.
            ENDIF.
            lv_in_block = abap_false.
          ENDIF.
          lv_insert = abap_true.
          CONTINUE.
        ENDIF.

        IF lv_insert = abap_true.
          lv_section = lv_section + 1.

          READ TABLE lt_diffs INTO ls_bcon INDEX lv_tabix + 8.
          IF sy-subrc <> 0.
            ls_bcon = ls_diff.
          ENDIF.

          CLEAR lv_beacon.
          IF ls_bcon-beacon > 0.
            READ TABLE lt_beacons INTO lv_beacon INDEX ls_bcon-beacon.
          ENDIF.
          IF lv_beacon IS INITIAL.
            lv_beacon = '---'.
          ENDIF.

          CLEAR ls_hunk.
          ls_hunk-section_num = lv_section.
          ls_hunk-beacon_text = lv_beacon.
          APPEND ls_hunk TO ls_info-hunks.

          lv_insert = abap_false.
          lv_in_block = abap_false.
        ENDIF.

        CASE ls_diff-result.
          WHEN zif_abapgit_definitions=>c_diff-insert
            OR zif_abapgit_definitions=>c_diff-delete
            OR zif_abapgit_definitions=>c_diff-update.
            IF lv_in_block = abap_false.
              CLEAR ls_subblock.
              ls_subblock-first_line = lv_tabix.
              ls_subblock-last_line  = lv_tabix.
              lv_in_block = abap_true.
            ELSE.
              ls_subblock-last_line = lv_tabix.
            ENDIF.
            IF ls_diff-result = zif_abapgit_definitions=>c_diff-insert.
              ls_subblock-inserted = ls_subblock-inserted + 1.
            ELSEIF ls_diff-result = zif_abapgit_definitions=>c_diff-delete.
              ls_subblock-deleted = ls_subblock-deleted + 1.
            ELSE.
              ls_subblock-inserted = ls_subblock-inserted + 1.
              ls_subblock-deleted  = ls_subblock-deleted + 1.
            ENDIF.

          WHEN OTHERS.
            IF lv_in_block = abap_true.
              READ TABLE ls_info-hunks ASSIGNING <ls_hunk>
                INDEX lines( ls_info-hunks ).
              IF sy-subrc = 0.
                APPEND ls_subblock TO <ls_hunk>-subblocks.
              ENDIF.
              lv_in_block = abap_false.
            ENDIF.
        ENDCASE.

        READ TABLE ls_info-hunks ASSIGNING <ls_hunk>
          INDEX lines( ls_info-hunks ).
        IF sy-subrc = 0.
          CASE ls_diff-result.
            WHEN zif_abapgit_definitions=>c_diff-insert.
              <ls_hunk>-inserted = <ls_hunk>-inserted + 1.
              ls_info-total_insert = ls_info-total_insert + 1.
            WHEN zif_abapgit_definitions=>c_diff-delete.
              <ls_hunk>-deleted = <ls_hunk>-deleted + 1.
              ls_info-total_delete = ls_info-total_delete + 1.
            WHEN zif_abapgit_definitions=>c_diff-update.
              <ls_hunk>-inserted = <ls_hunk>-inserted + 1.
              <ls_hunk>-deleted  = <ls_hunk>-deleted + 1.
              ls_info-total_insert = ls_info-total_insert + 1.
              ls_info-total_delete = ls_info-total_delete + 1.
          ENDCASE.
        ENDIF.

      ENDLOOP.

      IF lv_in_block = abap_true.
        READ TABLE ls_info-hunks ASSIGNING <ls_hunk>
          INDEX lines( ls_info-hunks ).
        IF sy-subrc = 0.
          APPEND ls_subblock TO <ls_hunk>-subblocks.
        ENDIF.
        lv_in_block = abap_false.
      ENDIF.

      APPEND ls_info TO rt_data.

    ENDLOOP.

  ENDMETHOD.


  METHOD render_nav_json.

    DATA lt_data       TYPE ty_file_info_tt.
    DATA lt_files_json TYPE string_table.
    DATA ls_info       TYPE ty_file_info.
    DATA ls_hunk       TYPE ty_hunk_info.
    DATA ls_sub        TYPE ty_subblock_info.
    DATA lv_file_json  TYPE string.
    DATA lv_first_h    TYPE abap_bool.
    DATA lv_first_s    TYPE abap_bool.

    lt_data = build_nav_data( it_diff_files ).

    LOOP AT lt_data INTO ls_info.
      lv_file_json = '{"idx":' && ls_info-file_index
        && ',"filename":"' && escape_json( ls_info-filename ) && '"'
        && ',"nfname":"' && escape_json( ls_info-nfname ) && '"'
        && ',"lstate":"' && ls_info-lstate && '"'
        && ',"rstate":"' && ls_info-rstate && '"'
        && ',"ins":' && ls_info-total_insert
        && ',"del":' && ls_info-total_delete
        && ',"hunks":['.

      lv_first_h = abap_true.
      LOOP AT ls_info-hunks INTO ls_hunk.
        IF lv_first_h = abap_false.
          lv_file_json = lv_file_json && ','.
        ENDIF.
        lv_first_h = abap_false.

        lv_file_json = lv_file_json
          && '{"sec":' && ls_hunk-section_num
          && ',"text":"' && escape_json( ls_hunk-beacon_text ) && '"'
          && ',"ins":' && ls_hunk-inserted
          && ',"del":' && ls_hunk-deleted
          && ',"subs":['.

        lv_first_s = abap_true.
        LOOP AT ls_hunk-subblocks INTO ls_sub.
          IF lv_first_s = abap_false.
            lv_file_json = lv_file_json && ','.
          ENDIF.
          lv_first_s = abap_false.

          lv_file_json = lv_file_json
            && '{"f":' && ls_sub-first_line
            && ',"l":' && ls_sub-last_line
            && ',"i":' && ls_sub-inserted
            && ',"d":' && ls_sub-deleted
            && '}'.
        ENDLOOP.

        lv_file_json = lv_file_json && ']}'.
      ENDLOOP.

      lv_file_json = lv_file_json && ']}'.
      APPEND lv_file_json TO lt_files_json.
    ENDLOOP.

    rv_json = |[{ concat_lines_of( table = lt_files_json sep = ',' ) }]|.

  ENDMETHOD.


  METHOD render_nav_data_script.

    DATA lv_json TYPE string.

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.

    lv_json = render_nav_json( it_diff_files ).

    ri_html->add( '<script>window.ortecPatchNav=' && lv_json && ';</script>' ).

  ENDMETHOD.


  METHOD render_styles.

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.

    ri_html->add( '<style>' ).
    ri_html->add( '.ortec-patch-container { display:flex; gap:0; align-items:flex-start; }' ).
    ri_html->add( '.ortec-diff-content { flex:1; min-width:0; overflow-x:auto; }' ).
    ri_html->add( '.ortec-sidebar {' ).
    ri_html->add( '  width:280px; min-width:180px; max-height:calc(100vh - 100px);' ).
    ri_html->add( '  overflow-y:auto; position:sticky; top:10px;' ).
    ri_html->add( '  border-right:1px solid var(--theme-container-border-color,#ddd);' ).
    ri_html->add( '  background:var(--theme-container-background-color,#f8f8f8);' ).
    ri_html->add( '  font-size:12px; z-index:5;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-resize-handle {' ).
    ri_html->add( '  width:5px; cursor:col-resize; background:#e0e0e0;' ).
    ri_html->add( '  flex-shrink:0; transition:background 0.15s;' ).
    ri_html->add( '  position:relative; z-index:10; align-self:stretch;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-resize-handle:hover, .ortec-resize-handle.dragging {' ).
    ri_html->add( '  background:var(--theme-color-link,#0070d2);' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-sidebar-header { padding:8px; border-bottom:1px solid var(--theme-container-border-color,#ddd); }' ).
    ri_html->add( '.ortec-sidebar-title { font-weight:bold; margin-bottom:6px; }' ).
    ri_html->add( '.ortec-filter {' ).
    ri_html->add( '  width:100%; box-sizing:border-box; padding:4px 6px;' ).
    ri_html->add( '  border:1px solid var(--theme-container-border-color,#ddd);' ).
    ri_html->add( '  border-radius:3px; font-size:12px;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-jump-nav { display:flex; gap:4px; margin-top:6px; }' ).
    ri_html->add( '.ortec-jump-btn {' ).
    ri_html->add( '  flex:1; padding:3px 6px; border:1px solid var(--theme-container-border-color,#ddd);' ).
    ri_html->add( '  border-radius:3px; background:var(--theme-container-background-color,#fff);' ).
    ri_html->add( '  cursor:pointer; font-size:11px; text-align:center; color:#333;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-jump-btn:hover { background:var(--theme-color-focus-bg,#e8f0fe); }' ).
    ri_html->add( '.ortec-sidebar-files { overflow-y:auto; }' ).
    ri_html->add( '.ortec-sidebar-file { border-bottom:1px solid var(--theme-container-border-color,#eee); }' ).
    ri_html->add( '.ortec-sidebar-file.ortec-active {' ).
    ri_html->add( '  background:var(--theme-color-focus-bg,#e8f0fe);' ).
    ri_html->add( '  border-left:3px solid var(--theme-color-link,#0070d2);' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-file-header {' ).
    ri_html->add( '  display:flex; align-items:center; padding:5px 8px; cursor:pointer; gap:4px;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-file-header:hover {' ).
    ri_html->add( '  background:var(--theme-color-focus-bg,rgba(0,0,0,0.05));' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-expand {' ).
    ri_html->add( '  cursor:pointer; width:14px; text-align:center; flex-shrink:0; font-size:10px; color:#666;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-file-cb, .ortec-hunk-cb, .ortec-sub-cb { flex-shrink:0; cursor:pointer; }' ).
    ri_html->add( '.ortec-file-name {' ).
    ri_html->add( '  flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis;' ).
    ri_html->add( '  white-space:nowrap; cursor:pointer;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-file-stats, .ortec-hunk-stats, .ortec-sub-stats {' ).
    ri_html->add( '  flex-shrink:0; font-size:11px; color:#666; margin-left:auto;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-hunk-stats { font-size:10px; color:#888; }' ).
    ri_html->add( '.ortec-sub-stats { font-size:10px; color:#999; }' ).
    ri_html->add( '.ortec-state-added { color:#28a745; }' ).
    ri_html->add( '.ortec-state-deleted { color:#d73a49; }' ).
    ri_html->add( '.ortec-state-modified { color:#0366d6; }' ).
    ri_html->add( '.ortec-hunks { padding-left:26px; }' ).
    ri_html->add( '.ortec-hunk {' ).
    ri_html->add( '  display:flex; align-items:center; padding:3px 8px; gap:4px; font-size:11px;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-hunk:hover { background:var(--theme-color-focus-bg,rgba(0,0,0,0.03)); }' ).
    ri_html->add( '.ortec-hunk-name {' ).
    ri_html->add( '  flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;' ).
    ri_html->add( '  cursor:pointer; font-weight:600; color:var(--theme-color-link,#0366d6);' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-hunk-name:hover { text-decoration:underline; }' ).
    ri_html->add( '.ortec-subblocks { padding-left:16px; }' ).
    ri_html->add( '.ortec-subblock {' ).
    ri_html->add( '  display:flex; align-items:center; padding:2px 6px; gap:4px; font-size:10px;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-subblock:hover { background:var(--theme-color-focus-bg,rgba(0,0,0,0.03)); }' ).
    ri_html->add( '.ortec-sub-label {' ).
    ri_html->add( '  flex:1; min-width:0; cursor:pointer; color:#555;' ).
    ri_html->add( '}' ).
    ri_html->add( '.ortec-sidebar-footer {' ).
    ri_html->add( '  padding:6px 8px; border-top:1px solid var(--theme-container-border-color,#ddd);' ).
    ri_html->add( '  font-size:11px; color:#666; position:sticky; bottom:0;' ).
    ri_html->add( '  background:var(--theme-container-background-color,#f8f8f8);' ).
    ri_html->add( '}' ).
    ri_html->add( '.diff thead.nav_line th {' ).
    ri_html->add( '  font-size:13px !important; font-weight:700 !important;' ).
    ri_html->add( '  padding:8px 10px !important;' ).
    ri_html->add( '  background:linear-gradient(to right, #eaf3ff, #f4f8ff) !important;' ).
    ri_html->add( '  border-top:2px solid var(--theme-color-link,#0070d2) !important;' ).
    ri_html->add( '  border-bottom:1px solid #d0e3f7 !important;' ).
    ri_html->add( '  color:#1a3a5c !important;' ).
    ri_html->add( '  letter-spacing:0.3px;' ).
    ri_html->add( '}' ).
    ri_html->add( '</style>' ).

  ENDMETHOD.


  METHOD render_scripts.

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.
    ri_html->set_title( 'ZCL_ABAPGIT_ORTEC_GIT_PATCH sidebar' ).

    ri_html->add( '(function(){' ).
    ri_html->add( '  "use strict";' ).
    ri_html->add( '  var navData = window.ortecPatchNav;' ).
    ri_html->add( '  if (!navData || navData.length === 0) return;' ).
    ri_html->add( '  var diffList = document.getElementById("diff-list");' ).
    ri_html->add( '  if (!diffList) return;' ).
    ri_html->add( '  var allDiffs = diffList.querySelectorAll(":scope > .diff");' ).
    ri_html->add( '  if (allDiffs.length === 0) return;' ).
    ri_html->add( '  var activeIdx = 0;' ).
    ri_html->add( '  var Q=String.fromCharCode(39);' ).
    ri_html->add( '  var lineCacheByFile=Object.create(null);' ).
    ri_html->add( '  var lineCacheByHunk=Object.create(null);' ).

    ri_html->add( '  function escHtml(s){' ).
    ri_html->add( '    return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");' ).
    ri_html->add( '  }' ).
    ri_html->add( '  function escAttr(s){return s.replace(/&/g,"&amp;").replace(/"/g,"&quot;");}' ).
    ri_html->add( '  function escSel(s){return s.replace(/[.#\[\]]/g,"\\$&");}' ).
    ri_html->add( '  function getFileLines(nfname){' ).
    ri_html->add( '    var k=nfname;' ).
    ri_html->add( '    if(!lineCacheByFile[k]){' ).
    ri_html->add( '      var nf=escSel(k);' ).
    ri_html->add( '      lineCacheByFile[k]=Array.from(document.querySelectorAll("input[id^="+Q+"patch_line_"+nf+Q+"]"));' ).
    ri_html->add( '    }' ).
    ri_html->add( '    return lineCacheByFile[k];' ).
    ri_html->add( '  }' ).
    ri_html->add( '  function getHunkLines(nfname,sec){' ).
    ri_html->add( '    var k=nfname+"|"+sec;' ).
    ri_html->add( '    if(!lineCacheByHunk[k]){' ).
    ri_html->add( '      var nf=escSel(nfname);' ).
    ri_html->add( '      lineCacheByHunk[k]=Array.from(document.querySelectorAll("input[id^="+Q+"patch_line_"+nf+"_"+sec+"_"+Q+"]"));' ).
    ri_html->add( '    }' ).
    ri_html->add( '    return lineCacheByHunk[k];' ).
    ri_html->add( '  }' ).
    ri_html->add( '  function stateClass(l,r){' ).
    ri_html->add( '    if(l==="A"||r==="A")return" ortec-state-added";' ).
    ri_html->add( '    if(l==="D"||r==="D")return" ortec-state-deleted";' ).
    ri_html->add( '    if(l==="M"||r==="M")return" ortec-state-modified";' ).
    ri_html->add( '    return "";' ).
    ri_html->add( '  }' ).

    ri_html->add( '  function buildSidebar(){' ).
    ri_html->add( '    var h="";' ).
    ri_html->add( '    h+="<div class=\"ortec-sidebar-header\">";' ).
    ri_html->add( '    h+="<div class=\"ortec-sidebar-title\">Files ("+navData.length+")</div>";' ).
    ri_html->add( '    h+="<input type=\"text\" id=\"ortec-filter\" class=\"ortec-filter\" placeholder=\"Filter files...\">";' ).
    ri_html->add( '    h+="<div class=\"ortec-jump-nav\">";' ).
    ri_html->add( '    h+="<button type=\"button\" class=\"ortec-jump-btn\" id=\"ortec-prev-change\" title=\"Previous change (Ctrl+Up)\">&#9650; Prev</button>";' ).
    ri_html->add( '    h+="<button type=\"button\" class=\"ortec-jump-btn\" id=\"ortec-next-change\" title=\"Next change (Ctrl+Down)\">&#9660; Next</button>";' ).
    ri_html->add( '    h+="</div>";' ).
    ri_html->add( '    h+="</div><div class=\"ortec-sidebar-files\" id=\"ortec-sidebar-files\">";' ).
    ri_html->add( '    for(var i=0;i<navData.length;i++){' ).
    ri_html->add( '      var f=navData[i];' ).
    ri_html->add( '      var ac=(i===0)?" ortec-active":"";' ).
    ri_html->add( '      var sc=stateClass(f.lstate,f.rstate);' ).
    ri_html->add( '      h+="<div class=\"ortec-sidebar-file"+ac+"\" data-idx=\""+i+"\" data-fname=\""+escAttr(f.filename)+"\">";' ).
    ri_html->add( '      h+="<div class=\"ortec-file-header\">";' ).
    ri_html->add( '      h+="<span class=\"ortec-expand\" data-idx=\""+i+"\">&#9656;</span>";' ).
    ri_html->add( '      h+="<input type=\"checkbox\" class=\"ortec-file-cb\" data-idx=\""+i+"\" data-nf=\""+escAttr(f.nfname)+"\">";' ).
    ri_html->add( '      h+="<span class=\"ortec-file-name"+sc+"\" data-idx=\""+i+"\" title=\""+escAttr(f.path+f.filename)+"\">"+escHtml(f.filename)+"</span>";' ).
    ri_html->add( '      h+="<span class=\"ortec-file-stats\">+"+f.ins+"/-"+f.del+"</span>";' ).
    ri_html->add( '      h+="</div>";' ).
    ri_html->add( '      if(f.hunks&&f.hunks.length>0){' ).
    ri_html->add( '        h+="<div class=\"ortec-hunks\" id=\"ortec-hunks-"+i+"\" style=\"display:none\">";' ).
    ri_html->add( '        for(var j=0;j<f.hunks.length;j++){' ).
    ri_html->add( '          var k=f.hunks[j];' ).
    ri_html->add( '          h+="<div class=\"ortec-hunk\" data-idx=\""+i+"\" data-sec=\""+k.sec+"\">";' ).
    ri_html->add( '          h+="<input type=\"checkbox\" class=\"ortec-hunk-cb\" data-idx=\""+i+"\" data-sec=\""+k.sec+"\" data-nf=\""+escAttr(f.nfname)+"\">";' ).
    ri_html->add( '          h+="<span class=\"ortec-hunk-name\">"+escHtml(k.text)+"</span>";' ).
    ri_html->add( '          h+="<span class=\"ortec-hunk-stats\">+"+k.ins+"/-"+k.del+"</span>";' ).
    ri_html->add( '          h+="</div>";' ).
    ri_html->add( '          if(k.subs&&k.subs.length>1){' ).
    ri_html->add( '            h+="<div class=\"ortec-subblocks\">";' ).
    ri_html->add( '            for(var s=0;s<k.subs.length;s++){' ).
    ri_html->add( '              var sb=k.subs[s];' ).
    ri_html->add( '              h+="<div class=\"ortec-subblock\" data-idx=\""+i+"\" data-sec=\""+k.sec+"\" data-sf=\""+sb.f+"\" data-sl=\""+sb.l+"\" data-nf=\""+escAttr(f.nfname)+"\">";' ).
    ri_html->add( '              h+="<input type=\"checkbox\" class=\"ortec-sub-cb\" data-idx=\""+i+"\" data-sec=\""+k.sec+"\" data-sf=\""+sb.f+"\" data-sl=\""+sb.l+"\" data-nf=\""+escAttr(f.nfname)+"\">";' ).
    ri_html->add( '              h+="<span class=\"ortec-sub-label\">Block "+(s+1)+"</span>";' ).
    ri_html->add( '              h+="<span class=\"ortec-sub-stats\">+"+sb.i+"/-"+sb.d+"</span>";' ).
    ri_html->add( '              h+="</div>";' ).
    ri_html->add( '            }' ).
    ri_html->add( '            h+="</div>";' ).
    ri_html->add( '          }' ).
    ri_html->add( '        }' ).
    ri_html->add( '        h+="</div>";' ).
    ri_html->add( '      }' ).
    ri_html->add( '      h+="</div>";' ).
    ri_html->add( '    }' ).
    ri_html->add( '    h+="</div><div class=\"ortec-sidebar-footer\">";' ).
    ri_html->add( '    h+="<span id=\"ortec-summary\">File 1 of "+navData.length+"</span>";' ).
    ri_html->add( '    h+="</div>";' ).
    ri_html->add( '    return h;' ).
    ri_html->add( '  }' ).

    ri_html->add( '  var container=document.createElement("div");' ).
    ri_html->add( '  container.id="ortec-patch-container";' ).
    ri_html->add( '  container.className="ortec-patch-container";' ).
    ri_html->add( '  var sidebar=document.createElement("div");' ).
    ri_html->add( '  sidebar.id="ortec-sidebar";' ).
    ri_html->add( '  sidebar.className="ortec-sidebar";' ).
    ri_html->add( '  sidebar.innerHTML=buildSidebar();' ).
    ri_html->add( '  var resizeHandle=document.createElement("div");' ).
    ri_html->add( '  resizeHandle.className="ortec-resize-handle";' ).
    ri_html->add( '  resizeHandle.title="Drag to resize";' ).
    ri_html->add( '  diffList.parentNode.insertBefore(container,diffList);' ).
    ri_html->add( '  container.appendChild(sidebar);' ).
    ri_html->add( '  container.appendChild(resizeHandle);' ).
    ri_html->add( '  container.appendChild(diffList);' ).
    ri_html->add( '  diffList.classList.add("ortec-diff-content");' ).
    ri_html->add( '  var el=container.parentElement;' ).
    ri_html->add( '  while(el&&el!==document.body&&el!==document.documentElement){' ).
    ri_html->add( '    var ov=getComputedStyle(el).overflow;' ).
    ri_html->add( '    if(ov!=="visible")el.style.overflow="visible";' ).
    ri_html->add( '    el=el.parentElement;' ).
    ri_html->add( '  }' ).

    ri_html->add( '  (function(){' ).
    ri_html->add( '    var dragging=false,startX=0,startW=0;' ).
    ri_html->add( '    resizeHandle.addEventListener("mousedown",function(e){' ).
    ri_html->add( '      dragging=true; startX=e.clientX; startW=sidebar.offsetWidth;' ).
    ri_html->add( '      resizeHandle.classList.add("dragging");' ).
    ri_html->add( '      document.body.style.cursor="col-resize";' ).
    ri_html->add( '      document.body.style.userSelect="none";' ).
    ri_html->add( '      e.preventDefault();' ).
    ri_html->add( '    });' ).
    ri_html->add( '    document.addEventListener("mousemove",function(e){' ).
    ri_html->add( '      if(!dragging)return;' ).
    ri_html->add( '      var newW=startW+(e.clientX-startX);' ).
    ri_html->add( '      if(newW<120)newW=120; if(newW>600)newW=600;' ).
    ri_html->add( '      sidebar.style.width=newW+"px";' ).
    ri_html->add( '      sidebar.style.minWidth=newW+"px";' ).
    ri_html->add( '    });' ).
    ri_html->add( '    document.addEventListener("mouseup",function(){' ).
    ri_html->add( '      if(!dragging)return;' ).
    ri_html->add( '      dragging=false;' ).
    ri_html->add( '      resizeHandle.classList.remove("dragging");' ).
    ri_html->add( '      document.body.style.cursor="";' ).
    ri_html->add( '      document.body.style.userSelect="";' ).
    ri_html->add( '    });' ).
    ri_html->add( '  })();' ).

    ri_html->add( '  for(var i=0;i<allDiffs.length;i++){' ).
    ri_html->add( '    allDiffs[i].setAttribute("data-ortec-idx",i);' ).
    ri_html->add( '    if(i>0) allDiffs[i].style.display="none";' ).
    ri_html->add( '  }' ).

    ri_html->add( '  function switchFile(idx){' ).
    ri_html->add( '    if(idx<0||idx>=allDiffs.length||idx===activeIdx) return;' ).
    ri_html->add( '    allDiffs[activeIdx].style.display="none";' ).
    ri_html->add( '    var oldSb=sidebar.querySelector(".ortec-sidebar-file.ortec-active");' ).
    ri_html->add( '    if(oldSb) oldSb.classList.remove("ortec-active");' ).
    ri_html->add( '    activeIdx=idx;' ).
    ri_html->add( '    allDiffs[idx].style.display="";' ).
    ri_html->add( '    var sel=".ortec-sidebar-file[data-idx="+Q+idx+Q+"]";' ).
    ri_html->add( '    var newSb=sidebar.querySelector(sel);' ).
    ri_html->add( '    if(newSb){' ).
    ri_html->add( '      newSb.classList.add("ortec-active");' ).
    ri_html->add( '      newSb.scrollIntoView({block:"nearest"});' ).
    ri_html->add( '    }' ).
    ri_html->add( '    var sumEl=document.getElementById("ortec-summary");' ).
    ri_html->add( '    if(sumEl) sumEl.textContent="File "+(idx+1)+" of "+navData.length;' ).
    ri_html->add( '  }' ).

    ri_html->add( '  function getChangeRows(){' ).
    ri_html->add( '    var el=allDiffs[activeIdx]; if(!el)return [];' ).
    ri_html->add( '    return Array.from(el.querySelectorAll("thead.nav_line"));' ).
    ri_html->add( '  }' ).
    ri_html->add( '  var currentChangeIdx=-1;' ).
    ri_html->add( '  function jumpChange(dir){' ).
    ri_html->add( '    var rows=getChangeRows();' ).
    ri_html->add( '    if(rows.length===0){' ).
    ri_html->add( '      var nIdx=activeIdx+dir;' ).
    ri_html->add( '      if(nIdx>=0&&nIdx<allDiffs.length){' ).
    ri_html->add( '        switchFile(nIdx);' ).
    ri_html->add( '        var nr=getChangeRows();' ).
    ri_html->add( '        currentChangeIdx=(dir===1)?-1:nr.length;' ).
    ri_html->add( '        jumpChange(dir);}' ).
    ri_html->add( '      return;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    currentChangeIdx+=dir;' ).
    ri_html->add( '    if(currentChangeIdx>=rows.length){' ).
    ri_html->add( '      if(activeIdx<allDiffs.length-1){switchFile(activeIdx+1);' ).
    ri_html->add( '        currentChangeIdx=-1;jumpChange(1);return;}' ).
    ri_html->add( '      currentChangeIdx=0;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    if(currentChangeIdx<0){' ).
    ri_html->add( '      if(activeIdx>0){switchFile(activeIdx-1);' ).
    ri_html->add( '        var pr=getChangeRows();currentChangeIdx=pr.length>0?pr.length-1:0;' ).
    ri_html->add( '        if(pr.length>0){var ci=currentChangeIdx;' ).
    ri_html->add( '          pr[ci].scrollIntoView({block:"center",behavior:"smooth"});' ).
    ri_html->add( '          pr[ci].style.outline="2px solid var(--theme-color-link,#0070d2)";' ).
    ri_html->add( '          setTimeout(function(){pr[ci].style.outline="";},1500);}' ).
    ri_html->add( '        return;}' ).
    ri_html->add( '      currentChangeIdx=rows.length-1;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    var ci=currentChangeIdx;' ).
    ri_html->add( '    rows[ci].scrollIntoView({block:"center",behavior:"smooth"});' ).
    ri_html->add( '    rows[ci].style.outline="2px solid var(--theme-color-link,#0070d2)";' ).
    ri_html->add( '    setTimeout(function(){rows[ci].style.outline="";},1500);' ).
    ri_html->add( '  }' ).

    ri_html->add( '  function toggleSubblock(nfname,sec,firstLine,lastLine,checked){' ).
    ri_html->add( '    for(var li=firstLine;li<=lastLine;li++){' ).
    ri_html->add( '      var cb=document.getElementById("patch_line_"+nfname+"_"+sec+"_"+li);' ).
    ri_html->add( '      if(cb) cb.checked=checked;' ).
    ri_html->add( '    }' ).
    ri_html->add( '  }' ).

    ri_html->add( '  function syncFileFromLines(idx){' ).
    ri_html->add( '    var f=navData[idx]; if(!f) return;' ).
    ri_html->add( '    var lines=getFileLines(f.nfname);' ).
    ri_html->add( '    var c=0,t=lines.length;' ).
    ri_html->add( '    for(var i=0;i<t;i++){if(lines[i].checked)c++;}' ).
    ri_html->add( '    var sbSel=".ortec-file-cb[data-idx="+Q+idx+Q+"]";' ).
    ri_html->add( '    var sbCb=sidebar.querySelector(sbSel);' ).
    ri_html->add( '    if(sbCb){sbCb.checked=(c===t&&t>0);sbCb.indeterminate=(c>0&&c<t);}' ).
    ri_html->add( '    var ilCb=document.getElementById("patch_file_"+f.nfname);' ).
    ri_html->add( '    if(ilCb){ilCb.checked=(c===t&&t>0);ilCb.indeterminate=(c>0&&c<t);}' ).
    ri_html->add( '  }' ).

    ri_html->add( '  function syncHunkCb(idx,nfname,sec){' ).
    ri_html->add( '    var lines=getHunkLines(nfname,sec);' ).
    ri_html->add( '    var c=0,t=lines.length;' ).
    ri_html->add( '    for(var i=0;i<t;i++){if(lines[i].checked)c++;}' ).
    ri_html->add( '    var hSel=".ortec-hunk-cb[data-idx="+Q+idx+Q+"][data-sec="+Q+sec+Q+"]";' ).
    ri_html->add( '    var hCb=sidebar.querySelector(hSel);' ).
    ri_html->add( '    if(hCb){hCb.checked=(c===t&&t>0);hCb.indeterminate=(c>0&&c<t);}' ).
    ri_html->add( '    var ilCb=document.getElementById("patch_section_"+nfname+"_"+sec);' ).
    ri_html->add( '    if(ilCb){ilCb.checked=(c===t&&t>0);ilCb.indeterminate=(c>0&&c<t);}' ).
    ri_html->add( '  }' ).

    ri_html->add( '  function syncSubCb(idx,nfname,sec,firstLine,lastLine){' ).
    ri_html->add( '    var c=0,t=0;' ).
    ri_html->add( '    for(var li=firstLine;li<=lastLine;li++){' ).
    ri_html->add( '      var cb=document.getElementById("patch_line_"+nfname+"_"+sec+"_"+li);' ).
    ri_html->add( '      if(cb){t++;if(cb.checked)c++;}' ).
    ri_html->add( '    }' ).
    ri_html->add( '    var sSel=".ortec-sub-cb[data-idx="+Q+idx+Q+"][data-sec="+Q+sec+Q+"][data-sf="+Q+firstLine+Q+"][data-sl="+Q+lastLine+Q+"]";' ).
    ri_html->add( '    var sCb=sidebar.querySelector(sSel);' ).
    ri_html->add( '    if(sCb){sCb.checked=(c===t&&t>0);sCb.indeterminate=(c>0&&c<t);}' ).
    ri_html->add( '  }' ).

    ri_html->add( '  function syncAllForFile(idx){' ).
    ri_html->add( '    var f=navData[idx]; if(!f) return;' ).
    ri_html->add( '    syncFileFromLines(idx);' ).
    ri_html->add( '    if(f.hunks){' ).
    ri_html->add( '      for(var j=0;j<f.hunks.length;j++){' ).
    ri_html->add( '        var k=f.hunks[j];' ).
    ri_html->add( '        syncHunkCb(idx,f.nfname,k.sec);' ).
    ri_html->add( '        if(k.subs&&k.subs.length>1){' ).
    ri_html->add( '          for(var s=0;s<k.subs.length;s++){' ).
    ri_html->add( '            syncSubCb(idx,f.nfname,k.sec,k.subs[s].f,k.subs[s].l);' ).
    ri_html->add( '          }' ).
    ri_html->add( '        }' ).
    ri_html->add( '      }' ).
    ri_html->add( '    }' ).
    ri_html->add( '  }' ).

    ri_html->add( '  sidebar.addEventListener("click",function(e){' ).
    ri_html->add( '    var t=e.target;' ).
    ri_html->add( '    if(t.classList.contains("ortec-file-name")){' ).
    ri_html->add( '      var fIdx=parseInt(t.getAttribute("data-idx"));' ).
    ri_html->add( '      switchFile(fIdx);' ).
    ri_html->add( '      currentChangeIdx=-1;' ).
    ri_html->add( '      allDiffs[fIdx].scrollIntoView({block:"start"});' ).
    ri_html->add( '      return;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    if(t.classList.contains("ortec-expand")){' ).
    ri_html->add( '      var xIdx=parseInt(t.getAttribute("data-idx"));' ).
    ri_html->add( '      var hd=document.getElementById("ortec-hunks-"+xIdx);' ).
    ri_html->add( '      if(hd){' ).
    ri_html->add( '        if(hd.style.display==="none"){hd.style.display="";t.innerHTML="&#9662;";}' ).
    ri_html->add( '        else{hd.style.display="none";t.innerHTML="&#9656;";}' ).
    ri_html->add( '      }' ).
    ri_html->add( '      return;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    if(t.classList.contains("ortec-file-cb")){' ).
    ri_html->add( '      var fIdx=parseInt(t.getAttribute("data-idx"));' ).
    ri_html->add( '      var nf=t.getAttribute("data-nf");' ).
    ri_html->add( '      var chk=t.checked;' ).
    ri_html->add( '      var ilCb=document.getElementById("patch_file_"+nf);' ).
    ri_html->add( '      if(ilCb&&ilCb.checked!==chk){ilCb.checked=chk;}' ).
    ri_html->add( '      var nfE=escSel(nf);' ).
    ri_html->add( '      var allL=getFileLines(nf);' ).
    ri_html->add( '      var allS=document.querySelectorAll("input[id^="+Q+"patch_section_"+nfE+Q+"]");' ).
    ri_html->add( '      for(var i=0;i<allL.length;i++)allL[i].checked=chk;' ).
    ri_html->add( '      for(var i=0;i<allS.length;i++)allS[i].checked=chk;' ).
    ri_html->add( '      var hSel=".ortec-hunk-cb[data-idx="+Q+fIdx+Q+"]";' ).
    ri_html->add( '      var hCbs=sidebar.querySelectorAll(hSel);' ).
    ri_html->add( '      for(var i=0;i<hCbs.length;i++){hCbs[i].checked=chk;hCbs[i].indeterminate=false;}' ).
    ri_html->add( '      var sSel=".ortec-sub-cb[data-idx="+Q+fIdx+Q+"]";' ).
    ri_html->add( '      var sCbs=sidebar.querySelectorAll(sSel);' ).
    ri_html->add( '      for(var i=0;i<sCbs.length;i++){sCbs[i].checked=chk;sCbs[i].indeterminate=false;}' ).
    ri_html->add( '      return;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    if(t.classList.contains("ortec-hunk-cb")){' ).
    ri_html->add( '      var hIdx=parseInt(t.getAttribute("data-idx"));' ).
    ri_html->add( '      var sec=t.getAttribute("data-sec");' ).
    ri_html->add( '      var nf=t.getAttribute("data-nf");' ).
    ri_html->add( '      var chk=t.checked;' ).
    ri_html->add( '      var ilCb=document.getElementById("patch_section_"+nf+"_"+sec);' ).
    ri_html->add( '      if(ilCb)ilCb.checked=chk;' ).
    ri_html->add( '      var allL=getHunkLines(nf,sec);' ).
    ri_html->add( '      for(var i=0;i<allL.length;i++)allL[i].checked=chk;' ).
    ri_html->add( '      var sSel=".ortec-sub-cb[data-idx="+Q+hIdx+Q+"][data-sec="+Q+sec+Q+"]";' ).
    ri_html->add( '      var sCbs=sidebar.querySelectorAll(sSel);' ).
    ri_html->add( '      for(var i=0;i<sCbs.length;i++){sCbs[i].checked=chk;sCbs[i].indeterminate=false;}' ).
    ri_html->add( '      syncFileFromLines(hIdx);' ).
    ri_html->add( '      return;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    if(t.classList.contains("ortec-sub-cb")){' ).
    ri_html->add( '      var sIdx=parseInt(t.getAttribute("data-idx"));' ).
    ri_html->add( '      var sec=t.getAttribute("data-sec");' ).
    ri_html->add( '      var nf=t.getAttribute("data-nf");' ).
    ri_html->add( '      var sf=parseInt(t.getAttribute("data-sf"));' ).
    ri_html->add( '      var sl=parseInt(t.getAttribute("data-sl"));' ).
    ri_html->add( '      var chk=t.checked;' ).
    ri_html->add( '      toggleSubblock(nf,sec,sf,sl,chk);' ).
    ri_html->add( '      syncHunkCb(sIdx,nf,sec);' ).
    ri_html->add( '      syncFileFromLines(sIdx);' ).
    ri_html->add( '      return;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    if(t.classList.contains("ortec-sub-label")){' ).
    ri_html->add( '      var p=t.closest(".ortec-subblock");' ).
    ri_html->add( '      if(!p) return;' ).
    ri_html->add( '      var sIdx=parseInt(p.getAttribute("data-idx"));' ).
    ri_html->add( '      var sec=p.getAttribute("data-sec");' ).
    ri_html->add( '      var sf=parseInt(p.getAttribute("data-sf"));' ).
    ri_html->add( '      var nf=p.getAttribute("data-nf");' ).
    ri_html->add( '      if(sIdx!==activeIdx){switchFile(sIdx);currentChangeIdx=-1;}' ).
    ri_html->add( '      setTimeout(function(){' ).
    ri_html->add( '        var lineEl=document.getElementById("patch_line_"+nf+"_"+sec+"_"+sf);' ).
    ri_html->add( '        if(lineEl){' ).
    ri_html->add( '          var row=lineEl.closest("tr");' ).
    ri_html->add( '          if(row){' ).
    ri_html->add( '            row.scrollIntoView({block:"center",behavior:"smooth"});' ).
    ri_html->add( '            row.style.outline="2px solid var(--theme-color-link,#0070d2)";' ).
    ri_html->add( '            setTimeout(function(){row.style.outline="";},1500);' ).
    ri_html->add( '          }' ).
    ri_html->add( '        }' ).
    ri_html->add( '      },50);' ).
    ri_html->add( '      return;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    if(t.classList.contains("ortec-hunk-name")){' ).
    ri_html->add( '      var p=t.parentElement;' ).
    ri_html->add( '      var nIdx=parseInt(p.getAttribute("data-idx"));' ).
    ri_html->add( '      var sec=p.getAttribute("data-sec");' ).
    ri_html->add( '      if(nIdx!==activeIdx){switchFile(nIdx);currentChangeIdx=-1;}' ).
    ri_html->add( '      setTimeout(function(){' ).
    ri_html->add( '        var nf=navData[nIdx].nfname;' ).
    ri_html->add( '        var secCb=document.getElementById("patch_section_"+nf+"_"+sec);' ).
    ri_html->add( '        if(secCb){' ).
    ri_html->add( '          var beacon=secCb.closest("thead");' ).
    ri_html->add( '          if(beacon){' ).
    ri_html->add( '            beacon.scrollIntoView({block:"start",behavior:"smooth"});' ).
    ri_html->add( '            beacon.style.outline="2px solid var(--theme-color-link,#0070d2)";' ).
    ri_html->add( '            setTimeout(function(){beacon.style.outline="";},1500);' ).
    ri_html->add( '          }' ).
    ri_html->add( '        }' ).
    ri_html->add( '      },50);' ).
    ri_html->add( '      return;' ).
    ri_html->add( '    }' ).
    ri_html->add( '  });' ).

    ri_html->add( '  var prevBtn=document.getElementById("ortec-prev-change");' ).
    ri_html->add( '  var nextBtn=document.getElementById("ortec-next-change");' ).
    ri_html->add( '  if(prevBtn) prevBtn.addEventListener("click",function(){jumpChange(-1);});' ).
    ri_html->add( '  if(nextBtn) nextBtn.addEventListener("click",function(){jumpChange(1);});' ).

    ri_html->add( '  diffList.addEventListener("click",function(e){' ).
    ri_html->add( '    if(!e.target||!e.target.id) return;' ).
    ri_html->add( '    if(e.target.id.indexOf("patch_")===0){' ).
    ri_html->add( '      setTimeout(function(){syncAllForFile(activeIdx);},10);' ).
    ri_html->add( '    }' ).
    ri_html->add( '  });' ).

    ri_html->add( '  var filterEl=document.getElementById("ortec-filter");' ).
    ri_html->add( '  if(filterEl){' ).
    ri_html->add( '    filterEl.addEventListener("input",function(){' ).
    ri_html->add( '      var v=this.value.toLowerCase();' ).
    ri_html->add( '      var files=sidebar.querySelectorAll(".ortec-sidebar-file");' ).
    ri_html->add( '      for(var i=0;i<files.length;i++){' ).
    ri_html->add( '        var fn=files[i].getAttribute("data-fname").toLowerCase();' ).
    ri_html->add( '        files[i].style.display=(v===""||fn.indexOf(v)>=0)?"":"none";' ).
    ri_html->add( '      }' ).
    ri_html->add( '    });' ).
    ri_html->add( '  }' ).

    ri_html->add( '  document.addEventListener("click",function(e){' ).
    ri_html->add( '    var a=e.target.closest("a[href]");' ).
    ri_html->add( '    if(!a) return;' ).
    ri_html->add( '    var href=a.getAttribute("href");' ).
    ri_html->add( '    if(a.id&&a.id.indexOf("li_jump_")===0){' ).
    ri_html->add( '      var txt=a.textContent||a.innerText||"";' ).
    ri_html->add( '      txt=txt.trim();' ).
    ri_html->add( '      if(txt){' ).
    ri_html->add( '        for(var i=0;i<allDiffs.length;i++){' ).
    ri_html->add( '          var df=allDiffs[i].getAttribute("data-file")||"";' ).
    ri_html->add( '          if(df&&df.indexOf(txt)>=0){' ).
    ri_html->add( '            e.preventDefault(); e.stopPropagation();' ).
    ri_html->add( '            switchFile(i);' ).
    ri_html->add( '            currentChangeIdx=-1;' ).
    ri_html->add( '            allDiffs[i].scrollIntoView({block:"start"});' ).
    ri_html->add( '            return;' ).
    ri_html->add( '          }' ).
    ri_html->add( '        }' ).
    ri_html->add( '      }' ).
    ri_html->add( '      return;' ).
    ri_html->add( '    }' ).
    ri_html->add( '    if(href&&href.indexOf("#")===0&&href.length>1){' ).
    ri_html->add( '      var targetId=href.substring(1);' ).
    ri_html->add( '      var targetEl=document.getElementById(targetId);' ).
    ri_html->add( '      if(targetEl){' ).
    ri_html->add( '        for(var i=0;i<allDiffs.length;i++){' ).
    ri_html->add( '          if(allDiffs[i]===targetEl||allDiffs[i].contains(targetEl)){' ).
    ri_html->add( '            e.preventDefault();' ).
    ri_html->add( '            switchFile(i);' ).
    ri_html->add( '            currentChangeIdx=-1;' ).
    ri_html->add( '            allDiffs[i].scrollIntoView({block:"start"});' ).
    ri_html->add( '            return;' ).
    ri_html->add( '          }' ).
    ri_html->add( '        }' ).
    ri_html->add( '      }' ).
    ri_html->add( '    }' ).
    ri_html->add( '  },true);' ).

    ri_html->add( '  document.addEventListener("keydown",function(e){' ).
    ri_html->add( '    if(e.ctrlKey&&e.key==="ArrowDown"){' ).
    ri_html->add( '      e.preventDefault(); jumpChange(1);' ).
    ri_html->add( '    } else if(e.ctrlKey&&e.key==="ArrowUp"){' ).
    ri_html->add( '      e.preventDefault(); jumpChange(-1);' ).
    ri_html->add( '    } else if(e.altKey&&e.key==="ArrowDown"){' ).
    ri_html->add( '      e.preventDefault();' ).
    ri_html->add( '      switchFile(activeIdx+1);' ).
    ri_html->add( '      if(allDiffs[activeIdx]) allDiffs[activeIdx].scrollIntoView({block:"start"});' ).
    ri_html->add( '      currentChangeIdx=-1;' ).
    ri_html->add( '    } else if(e.altKey&&e.key==="ArrowUp"){' ).
    ri_html->add( '      e.preventDefault();' ).
    ri_html->add( '      switchFile(activeIdx-1);' ).
    ri_html->add( '      if(allDiffs[activeIdx]) allDiffs[activeIdx].scrollIntoView({block:"start"});' ).
    ri_html->add( '      currentChangeIdx=-1;' ).
    ri_html->add( '    }' ).
    ri_html->add( '  });' ).

    ri_html->add( '  for(var i=0;i<navData.length;i++) syncAllForFile(i);' ).
    ri_html->add( '  var firstHunks=document.getElementById("ortec-hunks-0");' ).
    ri_html->add( '  if(firstHunks){firstHunks.style.display="";' ).
    ri_html->add( '    var exp0=sidebar.querySelector(".ortec-expand[data-idx="+Q+"0"+Q+"]");' ).
    ri_html->add( '    if(exp0) exp0.innerHTML="&#9662;";' ).
    ri_html->add( '  }' ).

    ri_html->add( '})();' ).

  ENDMETHOD.


ENDCLASS.

