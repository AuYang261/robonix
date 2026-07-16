uint32 NORMAL=0
uint32 FAULT=1
uint32 ESTOP=2

string key                   # root child path tail; root path tail when there are no direct children
string model                 # root/root-child ComponentStatus.model, "unknown" when empty
uint32 status                # global ESTOP/FAULT plus subtree error-fault/communication aggregate
BodyComponent[] components   # ComponentStatus descendants selected by root id path prefix
