
channel='234'
def test(a):

    global channel
    if a==1:
        channel='xxxx'

    b=channel+'23'

    return b

print(test(1))