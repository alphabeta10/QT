
def kl_manager_rate(p,q,b):
    return (b*p-q)/b


if __name__ == '__main__':
    cash = 50000
    p = 0.9
    q = 1-p
    b = 3500/3151
    x = kl_manager_rate(p,q,b)
    print(x*cash)

