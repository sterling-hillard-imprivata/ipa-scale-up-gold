## IPA load testing ## 
The goal is to do load testing

We want to do the steps BUT instead of for kafka-generator, we want to make sql queries to put into ClickHouse 
- download all proto schemas into kafka-generator
- update kafka-generator to generate IPA events
- build a direct-to-db batch feature for generator?
- update generator to use sampling of data for more realistic generator
- add a manual mode to kafka-generator to send one-off events to kakfa




## Topic to Table Mapping
astra-ps-patient-audit-events-v2 = patient_audit_events
astra-ps-activity-events = activity_events
astra-pa-data-import-events = data_import_events
astra-ps-face-biometric-events-v2 = face_biometric_events
astra-ps-configuration-audit-events = configuration_audit_events
astra-ps-photo-events-v2 = photo_events
astra-ps-outbound-events = hl7_outbound_events
