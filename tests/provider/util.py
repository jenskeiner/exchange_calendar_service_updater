from exchange_calendars_extensions.api.changes import ChangeSetDict
from yaml import safe_load

CHANGES = r'''XLON:
  add:
    - date: 2023-12-18
      type: holiday
      name: Added Holiday 1
    - date: 2023-12-19
      type: holiday
      name: Added Holiday 2
    - date: 2023-11-01
      type: special_open
      name: Added Special Open 1
      time: "10:00"
    - date: 2023-11-02
      type: special_open
      name: Added Special Open 2
      time: "11:00"
    - date: 2023-10-02
      type: special_close
      name: Added Special Close 1
      time: "10:00"
    - date: 2023-10-04
      type: special_close
      name: Added Special Close 2
      time: "11:00"
  remove:
    - 2023-12-25
'''

CHANGES_MODEL = ChangeSetDict(**safe_load(CHANGES))

CHANGES_ALT = CHANGES.replace('XLON', 'XETR')

CHANGES_ALT_MODEL = ChangeSetDict(**safe_load(CHANGES_ALT))
