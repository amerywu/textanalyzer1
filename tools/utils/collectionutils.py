
def _get_list_from_dict(adict, key):
    if key in adict:
        return adict[key]
    else:
        adict[key] = []
        return adict[key]

def _list_to_string( alist):
    strout = ""
    for s in alist:
        strout += s +"."
    return strout

def incrementing_key(key, adict):
    incrementer = 0
    incrementing_key =  key + "_" + str(incrementer)
    key_exists = incrementing_key in adict.keys()
    while key_exists:
        incrementer = incrementer + 1
        incrementing_key = key + "_" + str(incrementer)
        key_exists = incrementing_key in adict.keys()
    return incrementing_key

def get_top_incrementing_key(key, adict):
    incrementer = 0
    incrementing_key =  key + "_" + str(incrementer)
    key_exists = incrementing_key in adict.keys()
    while key_exists:
        incrementing_key = key + "_" + str(incrementer)

        incrementer = incrementer + 1
        key_exists = key + "_" + str(incrementer) in adict.keys()

    return incrementing_key