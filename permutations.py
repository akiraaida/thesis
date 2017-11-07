import csv
import sys

class Player:
    def __init__(self, pid, ping, rank):
        self.pid = pid
        self.ping = ping
        self.rank = rank

    def __repr__(self):
        return "Player: " + str(self.pid)

    def getPid(self):
        return self.pid

    def getPing(self):
        return self.ping

    def getRank(self):
        return self.rank

class PermutationBuilder:

    def __init__(self):
        self.teamCostMap = {}
        self.matchCostMap = {}

    def getTeamKeyAsList(self, team):
        pids = []
        for player in team:
            pids.append(player.getPid())
        pids.sort()
        return pids

    def getMatchKey(self, team1Key, team2Key):
        matchKey = []
        matchKey.append(team1Key) 
        matchKey.append(team2Key)
        matchKey.sort(key=sum)
        return str(matchKey)

    def calculateTeamCost(self, team):
        teamCost = 0
        for player in team:
            teamCost += player.getPing()
        return teamCost

    def getTeamCost(self, teamKey, team):
        if self.teamCostMap.get(teamKey) == None:
            teamCost = self.calculateTeamCost(team)
            self.teamCostMap[teamKey] = teamCost
            return teamCost
        else:
            return self.teamCostMap[teamKey]

    def getMatchCost(self, matchKey, team1Cost, team2Cost):
        if self.matchCostMap.get(matchKey) == None:
            matchCost = abs(team1Cost - team2Cost)
            self.matchCostMap[matchKey] = matchCost
            return matchCost
        else:
            return self.matchCostMap[matchKey]

    def calculatePairs1v1(self, players, cost, matches):
        if matches == None:
            matches = []
        for i in range(len(players)):
            for j in range(len(players)):
                if i == j:
                    continue
                team1 = [players[i]]
                team2 = [players[j]]
                team1Key = self.getTeamKeyAsList(team1)
                team2Key = self.getTeamKeyAsList(team2)
                matchKey = self.getMatchKey(team1Key, team2Key)
                team1Cost = self.getTeamCost(str(team1Key), team1)
                team2Cost = self.getTeamCost(str(team2Key), team2)
                matchCost = self.getMatchCost(matchKey, team1Cost, team2Cost)

                indices = [i, j]
                remainingList = [k for l, k in enumerate(players) if l not in indices]

                if len(remainingList) >= 2:
                    temp = matches[:]
                    temp.append([team1, team2])
                    self.calculatePairs1v1(remainingList, cost + matchCost, temp)
                else:
                    temp = matches[:]
                    temp.append([team1, team2])
                    print(temp)
                    print("End Cost: " + str(cost + matchCost))

players = []
counter = 0
with open('data.csv', 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        if row[1] == 'LeagueIndex':
            continue
        newPlayer = Player(counter, float(row[13]), row[1])
        players.append(newPlayer)
        counter += 1
builder = PermutationBuilder()
builder.calculatePairs1v1(players, 0, [])
