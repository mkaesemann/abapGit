METHOD decompress_binary_stream_git .

  DATA: inLen       TYPE i,
        outLen      TYPE i,
        redo        TYPE i VALUE 1,
        gzip_in_off TYPE i VALUE 0,
        l_xstr      TYPE xstring.

  FIELD-SYMBOLS: <fs> TYPE any.

  inLen  = gzip_in_len.
  outLen = me->out_buf_len.

  IF me->out_buf IS INITIAL.
    RAISE EXCEPTION TYPE cx_sy_missing_outbuf.
  ENDIF.

  ASSIGN me->out_buf->* TO <fs>.

  l_xstr = gzip_in+me->header_offset.

  DATA(crc32_enabled) = me->with_header.

  WHILE redo = 1.
    CALL METHOD me->_decompress_binary_stream
      EXPORTING
        gzip_in       = l_xstr
        gzip_in_len   = inLen
        ds_in         = me->ds
        raw_out_fill  = me->out_buf_fill
        stream_end    = 'X'
      IMPORTING
        raw_out       = <fs>
        raw_out_len   = outLen
      CHANGING
        gzip_in_off   = gzip_in_off
        crc32_enabled = crc32_enabled
        crc32_sum     = me->crc32.

    IF outLen = me->out_buf_len.
      me->total_len = me->total_len + outLen.
      me->out_buf_fill = 0.
      redo             = 1.
      IF gzip_in_off = 0.
        inLen = 0.
      ENDIF.
      CALL METHOD me->output_handler->use_out_buf
        EXPORTING
          out_buf     = <fs>
          out_buf_len = me->out_buf_len
          part        = me->part
          gzip_stream = me.
      me->part = 1.
    ELSE.
      redo = 0.
      IF outLen < 0.
        me->total_len = me->total_len + outLen.
        me->out_buf_fill = 0.
        inLen = 0.
        CALL METHOD me->output_handler->use_out_buf
          EXPORTING
            out_buf     = <fs>
            out_buf_len = me->out_buf_len
            part        = me->part
            gzip_stream = me.
        me->part = 1.
      ELSE.
        me->out_buf_fill = outLen.
      ENDIF.
    ENDIF.
  ENDWHILE.

  IF me->out_buf_fill > 0.
    me->total_len = me->total_len + outLen.
    CALL METHOD me->output_handler->use_out_buf
      EXPORTING
        out_buf     = <fs>
        out_buf_len = me->out_buf_fill
        part        = 2
        gzip_stream = me.
  ENDIF.

  " === THE KEY ADDITION: expose consumed input byte count ===
  ev_compressed_len = gzip_in_off.

  IF me->with_header = 'X'.
    DATA: l_x4   TYPE x LENGTH 4,
          l_byte TYPE x,
          l_ofs  TYPE i.

    l_ofs = xstrlen( gzip_in ) - 4.
    l_x4 = gzip_in+l_ofs(4).
    lcl_helper=>swap32( CHANGING f = l_x4 ).
    IF l_x4 <> me->total_len.
      RAISE EXCEPTION TYPE cx_parameter_invalid_range EXPORTING parameter = 'GZIP_IN'.
    ENDIF.
    SUBTRACT 4 FROM l_ofs.
    l_x4 = gzip_in+l_ofs(4).
    lcl_helper=>swap32( CHANGING f = l_x4 ).
    IF l_x4 <> me->crc32.
      RAISE EXCEPTION TYPE cx_parameter_invalid_range EXPORTING parameter = 'GZIP_IN'.
    ENDIF.
  ENDIF.

  CALL METHOD me->_close_ds_handler
    EXPORTING
      ds_in = me->ds.
  me->header_offset = 0.

ENDMETHOD.
