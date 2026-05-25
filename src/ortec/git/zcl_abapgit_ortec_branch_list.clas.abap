"! <p class="shorttext synchronized">ORTEC Git Branch Selection</p>
"! <p>Rich web-based branch picker with real-time filtering,
"! GitFlow group organisation, and HEAD/current-branch indicators.</p>
"!
"! <h2>Usage pattern</h2>
"! <ol>
"!   <li>Call <em>CREATE</em> to fetch branches and build the component.</li>
"!   <li>Wrap in <em>ZCL_ABAPGIT_GUI_PAGE_HOC</em> and push as a new page.</li>
"!   <li>When the user navigates back, check <em>IS_FULFILLED</em>,
"!       <em>WAS_CANCELLED</em>, and <em>GET_RESULT</em>.</li>
"! </ol>
CLASS zcl_abapgit_ortec_branch_list DEFINITION
  PUBLIC
  FINAL
  INHERITING FROM zcl_abapgit_gui_component
  CREATE PRIVATE.

  PUBLIC SECTION.

    INTERFACES zif_abapgit_gui_renderable.
    INTERFACES zif_abapgit_gui_event_handler.
    INTERFACES zif_abapgit_gui_page_title.

    "! <p class="shorttext synchronized">Create a new branch picker component</p>
    "! Fetches the remote branch list and groups branches by GitFlow prefix.
    "! @parameter iv_url | Repository URL used to fetch the remote branch list
    "! @parameter iv_default_branch | Full ref name of the currently tracked branch (highlighted)
    "! @parameter iv_show_new_option | Append a '+ create new ...' entry at the bottom
    "! @parameter iv_hide_branch | Full ref name to exclude from the list (e.g. the current branch for merge)
    "! @parameter iv_hide_head | Exclude the HEAD virtual ref entry when abap_true
    "! @parameter ro_picker | The constructed picker component; push via ZCL_ABAPGIT_GUI_PAGE_HOC
    CLASS-METHODS create
      IMPORTING
        !iv_url             TYPE string
        !iv_default_branch  TYPE string   OPTIONAL
        !iv_show_new_option TYPE abap_bool DEFAULT abap_false
        !iv_hide_branch     TYPE string   OPTIONAL
        !iv_hide_head       TYPE abap_bool DEFAULT abap_false
      RETURNING
        VALUE(ro_picker)    TYPE REF TO zcl_abapgit_ortec_branch_list
      RAISING
        zcx_abapgit_exception.

    "! <p class="shorttext synchronized">Constructor</p>
    "! @parameter iv_url | Repository URL used to fetch the remote branch list
    "! @parameter iv_default_branch | Full ref name of the currently tracked branch
    "! @parameter iv_show_new_option | Append a '+ create new ...' entry
    "! @parameter iv_hide_branch | Full ref name to exclude from the list
    "! @parameter iv_hide_head | Exclude the HEAD virtual ref when abap_true
    METHODS constructor
      IMPORTING
        !iv_url             TYPE string
        !iv_default_branch  TYPE string   OPTIONAL
        !iv_show_new_option TYPE abap_bool DEFAULT abap_false
        !iv_hide_branch     TYPE string   OPTIONAL
        !iv_hide_head       TYPE abap_bool DEFAULT abap_false
      RAISING
        zcx_abapgit_exception.

    "! <p class="shorttext synchronized">Whether the user has acted (selected or cancelled)</p>
    "! @parameter rv_yes | abap_true once the user either selects a branch or presses Back
    METHODS is_fulfilled
      RETURNING
        VALUE(rv_yes) TYPE abap_bool.

    "! <p class="shorttext synchronized">Whether the picker was cancelled without a selection</p>
    "! @parameter rv_yes | abap_true when the user pressed Back without selecting
    METHODS was_cancelled
      RETURNING
        VALUE(rv_yes) TYPE abap_bool.

    "! <p class="shorttext synchronized">Return the selected branch</p>
    "! Call only when IS_FULFILLED = abap_true and WAS_CANCELLED = abap_false.
    "! @parameter es_branch | The selected branch structure (name, sha1, display_name, is_head)
    METHODS get_result
      EXPORTING
        !es_branch TYPE zif_abapgit_git_definitions=>ty_git_branch.

  PROTECTED SECTION.
  PRIVATE SECTION.

    " ── Types ──────────────────────────────────────────────────────────────
    TYPES:
      BEGIN OF ty_group_item,
        branch     TYPE zif_abapgit_git_definitions=>ty_git_branch,
        global_idx TYPE i,
      END OF ty_group_item.
    TYPES ty_group_items_tt TYPE STANDARD TABLE OF ty_group_item WITH DEFAULT KEY.

    TYPES:
      BEGIN OF ty_branch_group,
        key   TYPE string,
        label TYPE string,
        items TYPE ty_group_items_tt,
      END OF ty_branch_group.
    TYPES ty_branch_groups_tt TYPE STANDARD TABLE OF ty_branch_group WITH DEFAULT KEY.

    " ── Constants ──────────────────────────────────────────────────────────
    CONSTANTS:
      BEGIN OF c_event,
        choose TYPE string VALUE 'ortec-branch-choose',
        back   TYPE string VALUE 'back',
      END OF c_event.

    CONSTANTS:
      BEGIN OF c_group,
        current     TYPE string VALUE 'current',
        head        TYPE string VALUE 'head',
        main        TYPE string VALUE 'main',
        development TYPE string VALUE 'development',
        feature     TYPE string VALUE 'feature',
        bugfix      TYPE string VALUE 'bugfix',
        hotfix      TYPE string VALUE 'hotfix',
        release     TYPE string VALUE 'release',
        releases    TYPE string VALUE 'releases',
        task        TYPE string VALUE 'task',
        support     TYPE string VALUE 'support',
        other       TYPE string VALUE 'other',
      END OF c_group.

    " ── Instance data ──────────────────────────────────────────────────────
    DATA mv_url             TYPE string.
    DATA mv_default_branch  TYPE string.
    DATA mv_show_new_option TYPE abap_bool.
    DATA mv_hide_branch     TYPE string.
    DATA mv_hide_head       TYPE abap_bool.
    DATA mt_branches        TYPE zif_abapgit_git_definitions=>ty_git_branch_list_tt.
    DATA mt_groups          TYPE ty_branch_groups_tt.
    DATA ms_result          TYPE zif_abapgit_git_definitions=>ty_git_branch.
    DATA mv_fulfilled       TYPE abap_bool.
    DATA mv_cancelled       TYPE abap_bool.

    " ── Private helpers ────────────────────────────────────────────────────
    METHODS fetch_and_group
      RAISING
        zcx_abapgit_exception.

    METHODS classify_branch
      IMPORTING
        !iv_branch_name  TYPE string OPTIONAL
        !iv_display_name TYPE string
        !iv_is_head      TYPE abap_bool DEFAULT abap_false
      RETURNING
        VALUE(rv_group)  TYPE string.

    METHODS render_search_bar
      RETURNING
        VALUE(ri_html) TYPE REF TO zif_abapgit_html.

    METHODS render_group
      IMPORTING
        !is_group      TYPE ty_branch_group
      RETURNING
        VALUE(ri_html) TYPE REF TO zif_abapgit_html.

    METHODS render_branch_item
      IMPORTING
        !is_item       TYPE ty_group_item
      RETURNING
        VALUE(ri_html) TYPE REF TO zif_abapgit_html.

    METHODS render_filter_script
      RETURNING
        VALUE(ri_html) TYPE REF TO zif_abapgit_html.

ENDCLASS.



CLASS zcl_abapgit_ortec_branch_list IMPLEMENTATION.


  METHOD create.
    CREATE OBJECT ro_picker
      EXPORTING
        iv_url             = iv_url
        iv_default_branch  = iv_default_branch
        iv_show_new_option = iv_show_new_option
        iv_hide_branch     = iv_hide_branch
        iv_hide_head       = iv_hide_head.
  ENDMETHOD.


  METHOD constructor.
    super->constructor( ).
    mv_url             = iv_url.
    mv_default_branch  = iv_default_branch.
    mv_show_new_option = iv_show_new_option.
    mv_hide_branch     = iv_hide_branch.
    mv_hide_head       = iv_hide_head.
    fetch_and_group( ).
  ENDMETHOD.


  METHOD fetch_and_group.

    DATA lo_branches    TYPE REF TO zif_abapgit_git_branch_list.
    DATA lv_head_symref TYPE string.
    DATA ls_new_branch  TYPE zif_abapgit_git_definitions=>ty_git_branch.
    DATA ls_group       TYPE ty_branch_group.
    DATA ls_item        TYPE ty_group_item.
    DATA lv_group_key   TYPE string.
    DATA lv_branch_idx  TYPE i.

    FIELD-SYMBOLS <ls_branch> LIKE LINE OF mt_branches.
    FIELD-SYMBOLS <ls_group>  LIKE LINE OF mt_groups.

    CLEAR mt_groups.

    lo_branches    = zcl_abapgit_git_factory=>get_git_transport( )->branches( mv_url ).
    mt_branches    = lo_branches->get_branches_only( ).
    lv_head_symref = lo_branches->get_head_symref( ).

    " Apply caller-requested filters
    IF mv_hide_branch IS NOT INITIAL.
      DELETE mt_branches WHERE name = mv_hide_branch.
    ENDIF.
    IF mv_hide_head = abap_true.
      DELETE mt_branches WHERE name    = zif_abapgit_git_definitions=>c_head_name
                            OR is_head = abap_true.
    ENDIF.

    " Remove HEAD duplicates: keep only the symbolic HEAD target
    LOOP AT mt_branches ASSIGNING <ls_branch>.
      IF <ls_branch>-name IS INITIAL.
        DELETE mt_branches INDEX sy-tabix.
      ELSEIF <ls_branch>-is_head = abap_true
            AND lv_head_symref      IS NOT INITIAL
            AND <ls_branch>-name   <> lv_head_symref.
        DELETE mt_branches INDEX sy-tabix.
      ENDIF.
    ENDLOOP.

    IF mt_branches IS INITIAL AND mv_show_new_option = abap_false.
      zcx_abapgit_exception=>raise( 'No branches are available to select' ).
    ENDIF.

    " Sort: current HEAD first, then alphabetically by display name
    SORT mt_branches BY is_head DESCENDING display_name ASCENDING.

    " Append virtual "create new branch" entry
    IF mv_show_new_option = abap_true.
      ls_new_branch-name         = zif_abapgit_popups=>c_new_branch_label.
      ls_new_branch-display_name = zif_abapgit_popups=>c_new_branch_label.
      APPEND ls_new_branch TO mt_branches.
    ENDIF.

    " Initialise groups in display order
    ls_group-key = c_group-current.     ls_group-label = 'Current'.          APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-head.        ls_group-label = 'HEAD'.             APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-main.        ls_group-label = 'Default / Stable'. APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-development. ls_group-label = 'Development'.      APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-feature.     ls_group-label = 'Features'.         APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-bugfix.      ls_group-label = 'Bug Fixes'.        APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-hotfix.      ls_group-label = 'Hot Fixes'.        APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-release.     ls_group-label = 'Release'.          APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-releases.    ls_group-label = 'Releases'.         APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-task.        ls_group-label = 'Tasks'.            APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-support.     ls_group-label = 'Support'.          APPEND ls_group TO mt_groups. CLEAR ls_group.
    ls_group-key = c_group-other.       ls_group-label = 'Other'.            APPEND ls_group TO mt_groups. CLEAR ls_group.

    " Assign branches to groups, preserving the 1-based mt_branches index
    LOOP AT mt_branches ASSIGNING <ls_branch>.
      lv_branch_idx = sy-tabix.
      lv_group_key = classify_branch(
        iv_branch_name  = <ls_branch>-name
        iv_display_name = <ls_branch>-display_name
        iv_is_head      = <ls_branch>-is_head ).
      READ TABLE mt_groups ASSIGNING <ls_group> WITH KEY key = lv_group_key.
      IF sy-subrc = 0.
        CLEAR ls_item.
        ls_item-branch     = <ls_branch>.
        ls_item-global_idx = lv_branch_idx.
        APPEND ls_item TO <ls_group>-items.
      ENDIF.
    ENDLOOP.

    " Remove groups that ended up with no branches
    DELETE mt_groups WHERE items IS INITIAL.

  ENDMETHOD.


  METHOD classify_branch.

    DATA lv_lower         TYPE string.
    DATA lv_default       TYPE string.
    DATA lv_default_disp  TYPE string.

    lv_lower        = to_lower( iv_display_name ).
    lv_default      = to_lower( mv_default_branch ).
    lv_default_disp = to_lower( zcl_abapgit_git_branch_utils=>get_display_name( mv_default_branch ) ).

    " The caller's selected/default branch is shown in a dedicated top group.
    IF mv_default_branch IS NOT INITIAL
        AND ( to_lower( iv_branch_name ) = lv_default OR lv_lower = lv_default_disp ).
      rv_group = c_group-current.
      RETURN.
    ENDIF.

    " HEAD virtual ref
    IF iv_is_head = abap_true AND lv_lower = to_lower( zif_abapgit_git_definitions=>c_head_name ).
      rv_group = c_group-head.
      RETURN.
    ENDIF.

    " Well-known stable branch names
    IF lv_lower = 'main' OR lv_lower = 'master' OR lv_lower = 'develop' OR lv_lower = 'development' OR lv_lower = 'trunk' OR lv_lower = 'release'.
      rv_group = c_group-main.
      RETURN.
    ENDIF.

    " GitFlow and ORTEC-specific prefix patterns
    IF lv_lower CP 'development/*'.
      rv_group = c_group-development.
    ELSEIF lv_lower CP 'feature/*' OR lv_lower CP 'feat/*'.
      rv_group = c_group-feature.
    ELSEIF lv_lower CP 'bugfix/*' OR lv_lower CP 'bug/*' OR lv_lower CP 'fix/*'.
      rv_group = c_group-bugfix.
    ELSEIF lv_lower CP 'hotfix/*' OR lv_lower CP 'hf/*'.
      rv_group = c_group-hotfix.
    ELSEIF lv_lower CP 'release/*' OR lv_lower CP 'rel/*'.
      rv_group = c_group-release.
    ELSEIF lv_lower CP 'releases/*'.
      rv_group = c_group-releases.
    ELSEIF lv_lower CP 'task/*'.
      rv_group = c_group-task.
    ELSEIF lv_lower CP 'support/*'.
      rv_group = c_group-support.
    ELSE.
      rv_group = c_group-other.
    ENDIF.

  ENDMETHOD.


  METHOD render_search_bar.

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.

    ri_html->add( '<div class="ortec-bp-filter" style="background:#fff;">' ).
    ri_html->add( '<input type="text" id="ortec-bp-input" autocomplete="off"' ).
    ri_html->add( ' placeholder="&#x1F50D; Filter branches&#x2026;"' ).
    ri_html->add( ' style="width:100%;padding:7px 10px;font-size:0.95em;' ).
    ri_html->add( ' color:#2c3e50;background:#fff;' ).
    ri_html->add( ' border:1px solid #b6bec8;border-radius:4px;' ).
    ri_html->add( ' box-sizing:border-box;margin-bottom:12px;" />' ).
    ri_html->add( '</div>' ).

  ENDMETHOD.


  METHOD render_branch_item.

    DATA lv_sha_short  TYPE string.
    DATA lv_is_current TYPE abap_bool.
    DATA lv_li_style   TYPE string.
    DATA lv_display_esc TYPE string.

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.

    " Short SHA (first 7 chars, like standard git output)
    IF is_item-branch-sha1 IS NOT INITIAL AND strlen( is_item-branch-sha1 ) >= 7.
      lv_sha_short = is_item-branch-sha1+0(7).
    ENDIF.

    lv_is_current = boolc( is_item-branch-name = mv_default_branch ).

    lv_li_style = 'list-style:none;padding:5px 8px;border-bottom:1px solid #e8edf2;'
               && 'display:flex;align-items:center;gap:6px;background:#fff;'.

    IF lv_is_current = abap_true.
      lv_li_style = lv_li_style && 'background:#eaf4fb;'.
    ENDIF.

    " HTML-safe display name for data attribute
    lv_display_esc = escape( val    = is_item-branch-display_name
                             format = cl_abap_format=>e_html_attr ).

    ri_html->add( |<li class="ortec-bp-item" data-idx="{ is_item-global_idx }"| ).
    ri_html->add( | data-display="{ lv_display_esc }" style="{ lv_li_style }">| ).

    " Clickable branch name
    ri_html->add_a(
      iv_txt   = is_item-branch-display_name
      iv_act   = c_event-choose
      iv_query = |IDX={ is_item-global_idx }|
      iv_style = 'flex:1;font-family:monospace;font-size:0.88em;'
              && 'color:#1f2933;text-decoration:none;overflow:hidden;'
              && 'text-overflow:ellipsis;white-space:nowrap;' ).

    " HEAD badge
    IF is_item-branch-is_head = abap_true.
      ri_html->add( '<span style="background:#27ae60;color:#fff;font-size:0.7em;' ).
      ri_html->add( 'padding:1px 6px;border-radius:10px;white-space:nowrap;">HEAD</span>' ).
    ENDIF.

    " Current / tracked branch badge
    IF lv_is_current = abap_true.
      ri_html->add( '<span style="background:#2980b9;color:#fff;font-size:0.7em;' ).
      ri_html->add( 'padding:1px 6px;border-radius:10px;white-space:nowrap;">current</span>' ).
    ENDIF.

    " Short commit hash
    IF lv_sha_short IS NOT INITIAL.
      ri_html->add( |<span style="color:#667085;font-size:0.75em;| ).
      ri_html->add( |font-family:monospace;white-space:nowrap;">{ lv_sha_short }</span>| ).
    ENDIF.

    ri_html->add( '</li>' ).

  ENDMETHOD.


  METHOD render_group.

    DATA lv_count TYPE i.
    DATA lv_open  TYPE string.
    FIELD-SYMBOLS <ls_item> LIKE LINE OF is_group-items.

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.

    lv_count = lines( is_group-items ).
    IF lv_count = 0.
      RETURN.
    ENDIF.

    IF is_group-key = c_group-current OR is_group-key = c_group-head.
      lv_open = ' open'.
    ENDIF.

    ri_html->add( |<details class="ortec-bp-group" data-group="{ is_group-key }"{ lv_open }| ).
    ri_html->add( ' style="margin-bottom:14px;background:#fff;">' ).

    " Collapsible group header
    ri_html->add( '<summary style="font-size:0.75em;font-weight:bold;color:#4f5b66;' ).
    ri_html->add( 'text-transform:uppercase;letter-spacing:0.06em;' ).
    ri_html->add( 'padding:5px 8px;background:#f1f4f7;border:1px solid #d8dee6;' ).
    ri_html->add( 'border-radius:3px;margin-bottom:2px;display:flex;' ).
    ri_html->add( 'justify-content:space-between;align-items:center;' ).
    ri_html->add( 'cursor:pointer;user-select:none;">' ).
    ri_html->add( |<span>{ is_group-label }</span>| ).
    ri_html->add( '<span class="ortec-bp-gcnt" style="background:#d8dee6;color:#4f5b66;' ).
    ri_html->add( |border-radius:10px;padding:1px 8px;font-size:0.9em;">{ lv_count }</span>| ).
    ri_html->add( '</summary>' ).

    " Branch items
    ri_html->add( '<ul style="margin:0;padding:0;background:#fff;">' ).
    LOOP AT is_group-items ASSIGNING <ls_item>.
      ri_html->add( render_branch_item( <ls_item> ) ).
    ENDLOOP.
    ri_html->add( '</ul>' ).

    ri_html->add( '</details>' ).

  ENDMETHOD.


  METHOD render_filter_script.

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.
    ri_html->set_title( 'OrtecBranchPicker' ).

    " Immediately-invoked function to keep scope clean
    ri_html->add( '(function() {' ).
    ri_html->add( '  var inp = document.getElementById("ortec-bp-input");' ).
    ri_html->add( '  if (!inp) return;' ).
    ri_html->add( '  inp.focus();' ).
    ri_html->add( '' ).
    ri_html->add( '  function applyFilter() {' ).
    ri_html->add( '    var q = inp.value.toLowerCase();' ).
    ri_html->add( '    var groups = document.querySelectorAll(".ortec-bp-group");' ).
    ri_html->add( '    groups.forEach(function(grp) {' ).
    ri_html->add( '      var vis = 0;' ).
    ri_html->add( '      grp.querySelectorAll(".ortec-bp-item").forEach(function(li) {' ).
    ri_html->add( '        var d = (li.getAttribute("data-display") || "").toLowerCase();' ).
    ri_html->add( '        var show = !q || d.indexOf(q) !== -1;' ).
    ri_html->add( '        li.style.display = show ? "" : "none";' ).
    ri_html->add( '        if (show) vis++;' ).
    ri_html->add( '      });' ).
    ri_html->add( '      var cntEl = grp.querySelector(".ortec-bp-gcnt");' ).
    ri_html->add( '      if (cntEl) cntEl.textContent = vis;' ).
    ri_html->add( '      grp.style.display = vis > 0 ? "" : "none";' ).
    ri_html->add( '      if (q && vis > 0) grp.open = true;' ).
    ri_html->add( '    });' ).
    ri_html->add( '  }' ).
    ri_html->add( '' ).
    ri_html->add( '  inp.addEventListener("input", applyFilter);' ).
    ri_html->add( '' ).
    ri_html->add( '  inp.addEventListener("keydown", function(e) {' ).
    ri_html->add( '    if (e.key === "Enter") {' ).
    ri_html->add( '      // Click first visible branch link' ).
    ri_html->add( '      var items = document.querySelectorAll(".ortec-bp-item");' ).
    ri_html->add( '      for (var i = 0; i < items.length; i++) {' ).
    ri_html->add( '        if (items[i].style.display !== "none" && items[i].offsetParent !== null) {' ).
    ri_html->add( '          var lnk = items[i].querySelector("a");' ).
    ri_html->add( '          if (lnk) { e.preventDefault(); lnk.click(); }' ).
    ri_html->add( '          break;' ).
    ri_html->add( '        }' ).
    ri_html->add( '      }' ).
    ri_html->add( '    } else if (e.key === "Escape") {' ).
    ri_html->add( '      var bk = document.querySelector(".ortec-bp-back");' ).
    ri_html->add( '      if (bk) bk.click();' ).
    ri_html->add( '    }' ).
    ri_html->add( '  });' ).
    ri_html->add( '})();' ).

  ENDMETHOD.


  METHOD is_fulfilled.
    rv_yes = mv_fulfilled.
  ENDMETHOD.


  METHOD was_cancelled.
    rv_yes = mv_cancelled.
  ENDMETHOD.


  METHOD get_result.
    es_branch = ms_result.
  ENDMETHOD.


  METHOD zif_abapgit_gui_page_title~get_page_title.
    rv_title = 'Select Branch'.
  ENDMETHOD.


  METHOD zif_abapgit_gui_renderable~render.

    FIELD-SYMBOLS <ls_group> LIKE LINE OF mt_groups.

    register_handlers( ).

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.

    ri_html->add( '<div class="ortec-branch-picker" style="' ).
    ri_html->add( 'background:#fff;color:#2c3e50;border:1px solid #d0d7de;' ).
    ri_html->add( 'border-radius:6px;box-shadow:0 12px 30px rgba(0,0,0,0.28);' ).
    ri_html->add( 'padding:0;max-width:760px;width:100%;height:560px;' ).
    ri_html->add( 'font-family:sans-serif;display:flex;flex-direction:column;' ).
    ri_html->add( 'box-sizing:border-box;overflow:hidden;">' ).

    " ── Search / filter bar ──────────────────────────────────────────────
    ri_html->add( '<div style="padding:16px 16px 0 16px;background:#fff;flex:0 0 auto;">' ).
    ri_html->add( render_search_bar( ) ).
    ri_html->add( '</div>' ).

    " ── Grouped branch list: only this area scrolls ──────────────────────
    ri_html->add( '<div id="ortec-bp-list" style="' ).
    ri_html->add( 'overflow-y:auto;overflow-x:hidden;flex:1 1 auto;' ).
    ri_html->add( 'padding:0 16px 8px 16px;background:#fff;box-sizing:border-box;">' ).
    LOOP AT mt_groups ASSIGNING <ls_group>.
      ri_html->add( render_group( <ls_group> ) ).
    ENDLOOP.
    ri_html->add( '</div>' ).

    " ── Back / cancel button: always visible ─────────────────────────────
    ri_html->add( '<div style="padding:10px 16px 14px 16px;background:#fff;' ).
    ri_html->add( 'border-top:1px solid #d0d7de;flex:0 0 auto;box-sizing:border-box;">' ).
    ri_html->add_a(
      iv_txt   = 'Back'
      iv_act   = c_event-back
      iv_class = 'button ortec-bp-back' ).
    ri_html->add( '</div>' ).

    ri_html->add( '</div>' ).

    register_deferred_script( render_filter_script( ) ).

  ENDMETHOD.


  METHOD zif_abapgit_gui_event_handler~on_event.

    DATA lv_idx    TYPE i.
    DATA ls_branch TYPE zif_abapgit_git_definitions=>ty_git_branch.

    CASE ii_event->mv_action.

      WHEN c_event-choose.

        lv_idx = ii_event->query( )->get( 'IDX' ).
        READ TABLE mt_branches INTO ls_branch INDEX lv_idx.

        IF sy-subrc = 0.
          ms_result    = ls_branch.
          mv_fulfilled = abap_true.
          rs_handled-state = zcl_abapgit_gui=>c_event_state-go_back.
        ELSE.
          " Index out of range; should not happen in practice
          rs_handled-state = zcl_abapgit_gui=>c_event_state-re_render.
        ENDIF.

      WHEN c_event-back OR zif_abapgit_definitions=>c_action-go_back.

        mv_fulfilled = abap_true.
        mv_cancelled = abap_true.
        rs_handled-state = zcl_abapgit_gui=>c_event_state-go_back.

      WHEN OTHERS.

        rs_handled-state = zcl_abapgit_gui=>c_event_state-not_handled.

    ENDCASE.

  ENDMETHOD.

ENDCLASS.
