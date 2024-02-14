@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'Transport Requests & Tasks'
define view entity ZPI_TransportRequests
  as select from           e070
    left outer to one join e07t as DescriptionEN       on  DescriptionEN.trkorr = e070.trkorr
                                                       and DescriptionEN.langu  = 'E'
    left outer to one join e07t as DescriptionDE       on  DescriptionDE.trkorr = e070.trkorr
                                                       and DescriptionDE.langu  = 'D'
    left outer to one join e07t as ParentDescriptionEN on  ParentDescriptionEN.trkorr = e070.strkorr
                                                       and ParentDescriptionEN.langu  = 'E'
    left outer to one join e07t as ParentDescriptionDE on  ParentDescriptionDE.trkorr = e070.strkorr
                                                       and ParentDescriptionDE.langu  = 'D'
{
  key e070.trkorr                  as Request,
      left(e070.trkorr,3)          as SystemId,
      e070.trfunction              as Function,
      e070.trstatus                as Status,
      e070.as4user                 as UserName,
      e070.as4date                 as ChangedOn,
      e070.as4time                 as ChangeAt,
      dats_tims_to_tstmp( e070.as4date,
                                  e070.as4time,
                                  abap_system_timezone( $session.client,'NULL' ),
                                  $session.client,
                           'NULL') as LastChanged,
      e070.strkorr                 as ParentRequest,
      case
        when DescriptionEN.as4text is not null
          then DescriptionEN.as4text
        when DescriptionDE.as4text is not null
          then DescriptionDE.as4text
      end                          as Description,
      case
        when ParentDescriptionEN.as4text is not null
          then ParentDescriptionEN.as4text
        when ParentDescriptionDE.as4text is not null
          then ParentDescriptionDE.as4text
      end                          as ParentDescription,
      case
        when left(DescriptionEN.as4text, 3) = 'OS4'
          or left(DescriptionDE.as4text, 3) = 'OS4'
          or left(ParentDescriptionEN.as4text, 3) = 'OS4'
          or left(ParentDescriptionDE.as4text, 3) = 'OS4'
          then 'X'
        else ''
      end                          as IsOS4Request
}
where
  e070.korrdev = 'SYST'
