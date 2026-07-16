float32 soc_percent          # first power source soc_percent, -1 if unavailable
float32 voltage              # first power source voltage (V), -1 if unavailable
bool charging                # first power source current > 0; false when nonpositive or unavailable
int64 remaining_s            # first power source estimated remaining seconds, -1 if unavailable
