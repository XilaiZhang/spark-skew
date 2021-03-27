from random import randint

words=["en", "spanish", "trukish", "greek", "it", "gibber", "ish","non","sense"]

for k in range(50000000):
    l=randint(1,8)
    word=""
    for i in range(l):
        choice=randint(0,8)
        word+=words[choice]
    print word


