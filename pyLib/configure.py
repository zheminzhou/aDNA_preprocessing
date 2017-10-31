import os, collections, json

def deep_update(d, u):
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = deep_update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def tonum(value) :
    if isinstance(value, basestring) :
        try :
            return int(value)
        except ValueError :
            try :
                return float(value)
            except ValueError :
                return value
    else :
        return value
def load_version(config, version='') :
    new_config = {}
    deep_update(new_config, config)
    if version == '':
        if 'current_version' in config :
            version = str(config['current_version'])
        else :
            return config
    else :
        version = str(version)
    if 'versions' not in config or version not in config['versions'] :
        raise

    vpoint = config['versions'][version]
    deep_update(new_config, vpoint)
    return new_config

def configure_reader(filename, type_check=True, append=True) :
    with open(filename, 'r') as fin:
        configure = {}
        region = None
        current = configure
        for l in fin:
            line = l.split('#',1)[0].strip()
            if line.find('{HOME}') >= 0 :
                line = line.replace('{HOME}', os.path.expanduser('~'))
            if len(line) < 1 : continue
            if line[0] == '[' :
                current = configure
                region = line[1:].split(']', 1)[0]
                reg = region.split('/')
                for r in reg:
                    if r == '' : continue
                    if r not in current :
                        current[r] = {}
                    current = current[r]
                if '=' in line :
                    filein = line.split('=', 1)[1].strip()
                    if os.path.isfile(filein) :
                        deep_update(current, configure_reader(filein, type_check, append))
            else:
                parts = line.split('=', 1)
                if len(parts) <= 1:
                    parts = line.split(':', 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()
                    if type_check :
                        if (value.startswith('[') and value.endswith(']')) or (value.startswith('{') and value.endswith('}')) :
                            try :
                                value = json.loads(value)
                            except :
                                pass
                        elif value.upper() in ('FALSE', 'TRUE') :
                            value = bool(value)
                        else :
                            value = tonum(value)
                        current[parts[0].strip()] = value
    return configure

