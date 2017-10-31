import sys
from time import strftime

log_env = sys.stdout

def logger(info, stream = None) :
    global log_env
    if stream != None :
        log_env = stream
    if info == None or log_env == None :
        return
    time = strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(log_env, str) :
        with open(log_env, 'ab') as fout:
            fout.write('{0}\t{1}\n'.format(time, info.rstrip('\n')))
            fout.flush()
    else :
        log_env.write('{0}\t{1}\n'.format(time, info.rstrip('\n')))
        log_env.flush()


