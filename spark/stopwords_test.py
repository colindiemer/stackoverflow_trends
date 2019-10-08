l = []
with open("stopwords_1000.txt", "r") as f:
    for line in f:
        l.append(line)
l = [word.strip() for word in l]
print(l[:10])
