import csv
import math

class Player:
    def __init__(self, index, pid, ping, skill):
        self.index = index
        self.pid = pid
        self.ping = ping
        self.skill = skill
        self.distance = []

    def __str__(self):
        return "Player: " + str(self.pid) + ", Ping: " + str(self.ping) + ", Skill: " + str(self.skill)

    def __repr__(self):
        return "Player: " + str(self.pid) + ", Ping: " + str(self.ping) + ", Skill: " + str(self.skill)

    def setDistance(self, distance):
        self.distance.append(distance)

    def clearDistance(self):
        self.distance = []

    def setIndex(self, index):
        self.index = index

    def getIndex(self):
        return self.index

    def setPid(self, pid):
        self.pid = pid

    def getDistance(self):
        return self.distance

    def getPid(self):
        return self.pid

    def getPing(self):
        return self.ping

    def getSkill(self):
        return self.skill

class EuclideanDistance:
    def __init__(self, player1, player2, distance, similarity):
        self.player1 = player1
        self.player2 = player2
        self.distance = distance
        self.similarity = similarity

    def getDistance(self):
        return self.distance

    def getSimilarity(self):
        return self.similarity

    def getPlayers(self):
        return [self.player1, self.player2]

    def __str__(self):
        return str(self.player1) + " & " + str(self.player2)

def getPlayers():
    players = []
    counter = 0
    with open('data.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[1] == 'LeagueIndex':
                continue
            player = Player(counter, counter, float(row[13]), float(row[1]))
            players.append(player)
            counter += 1
    return players

def calculateEuclideanDistance(player1, player2): 
    distance = (player1.getPing() - player2.getPing())**2 + (player1.getSkill() - player2.getSkill())**2
    return math.sqrt(distance)







players = getPlayers()



while len(players) >= 1:
    
    count = 0
    for player in players:
        player.setIndex(count)
        player.clearDistance()
        count += 1

    for i in range(len(players)):
        for j in range(i + 1, len(players)):
            distance = calculateEuclideanDistance(players[i], players[j])
            euclideanDistance = EuclideanDistance(players[i], players[j], distance, 1 / (1 + distance))
            players[i].setDistance(euclideanDistance)
            players[j].setDistance(euclideanDistance)

    bestMatch = None
    highestSimilarity = 0
    for player in players:
        distances = player.getDistance()
        for distance in distances:
            if highestSimilarity < distance.getSimilarity():
                highestSimilarity = distance.getSimilarity()
                bestMatch = distance

    print(bestMatch)

    bestPlayers = bestMatch.getPlayers()
    playersToBeRemoved = []
    for bestPlayer in bestPlayers:
        playersToBeRemoved.append(bestPlayer.getIndex())
    players = [k for l, k in enumerate(players) if l not in playersToBeRemoved]
    count += 1
