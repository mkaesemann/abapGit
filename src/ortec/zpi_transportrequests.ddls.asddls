@AccessControl.authorizationCheck: #NOT_REQUIRED
@EndUserText.label: 'Transport Requests & Tasks'
define view entity ZPI_TransportRequests
  as select from e070
{
  key trkorr                       as Request,
      left(trkorr,3)               as SystemId,
      trfunction                   as Function,
      trstatus                     as Status,
      as4user                      as UserName,
      as4date                      as ChangedOn,
      as4time                      as ChangeAt,
      dats_tims_to_tstmp( as4date,
                                  as4time,
                                  abap_system_timezone( $session.client,'NULL' ),
                                  $session.client,
                           'NULL') as LastChanged,
      strkorr                      as ParentRequest
}
where
  korrdev = 'SYST'
