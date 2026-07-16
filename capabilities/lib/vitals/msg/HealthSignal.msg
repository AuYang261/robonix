uint8 OK=0
uint8 WARN=1
uint8 ERROR=2
uint8 STALE=3
string key                  # stable health-signal key, e.g. "body/arm/joint_1/motor_temp"
uint8 status                # OK / WARN / ERROR / STALE
string detail               # threshold/fault/communication/quality explanation; may be empty for OK
float32 observed_value      # observed scalar or emitted status value
float32 reference_value     # crossed scalar limit, 0/1 status target, or -1 when not applicable
