from random import randint, choices
states = ["PR","CA"]
stateWeights = [0.95, 0.05]
zipcodes=["704","88"]
zipWeights = [0.7, 0.3]

print("State,Zipcode")
for k in range(5000000):
    line = choices(states, stateWeights)[0]+","+choices(zipcodes, zipWeights)[0]
    print(line)



