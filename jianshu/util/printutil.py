from termcolor import cprint


def eprint(msg):
    # print(*args, file=sys.stderr, **kwargs)
    cprint(msg, 'red')


def iprint(msg):
    cprint(msg, 'yellow')