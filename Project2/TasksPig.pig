-- data files
friendsRaw = LOAD '/user/cs4433/project/facebook_analytics/friends.csv' USING PigStorage(',') AS (friendRel, personID, myFriend, dateOfFriend, description);
pagesRaw = LOAD '/user/cs4433/project/facebook_analytics/pages.csv' USING PigStorage(',') AS (personID, name, nationality, countryCode, hobby);
accessRaw = LOAD '/user/cs4433/project/facebook_analytics/access_logs.csv' USING PigStorage(',') AS (accessID, byWho, whatPage, typeOfAccess, accessTime);


-- TaskA
clean1 = FILTER pagesRaw BY nationality == 'Grenada';
data1 = FOREACH clean1 GENERATE name, hobby;
STORE data1 INTO '/user/cs4433/project/facebook_analytics/output/users.csv' USING PigStorage(',');


-- TaskB
accesses = GROUP accessRaw BY whatPage;
pageCounts = FOREACH accesses GENERATE group AS whatPage, COUNT(accessRaw) AS pagecount;
sort2 = ORDER pageCounts BY pageCount DESC;
topTen = LIMIT sort2 10;
joinData2 = JOIN topTen BY whatPage, pagesRaw BY personID;
data2 = FOREACH joinData2 GENERATE
    pagesRaw::personID AS personID, pagesRaw::name AS name, pagesRaw::nationality AS nationality;
STORE data2 INTO '/user/cs4433/project/facebook_analytics/output/top10.csv' USING PigStorage(',');


-- TaskC
countries = GROUP pagesRaw BY countryCode;
data3 = FOREACH countries GENERATE group AS countryCode, COUNT(pagesRaw) AS countryCounts;
STORE data3 INTO '/user/cs4433/project/facebook_analytics/output/countryCounts.csv' USING PigStorage(',');


-- TaskD
friends = GROUP friendsRaw BY myFriend;
data4 = FOREACH friends GENERATE group AS personID, COUNT(friendsRaw) AS count;
join4 = JOIN pagesRaw BY personID LEFT OUTER, data4 BY personID;
fdata = FOREACH join4 GENERATE
    pagesRaw::personID AS personID,
    (data4::count IS NULL ? 0 : data4::count) AS count;
STORE fdata INTO '/user/cs4433/project/facebook_analytics/output/connectedness.csv' USING PigStorage(',');


-- TaskE
owner = GROUP accessRaw BY byWho;
unique = GROUP accessRaw BY (byWho, whatPage);
adata = FOREACH owner GENERATE
    group AS byWho,
    COUNT(accessRaw) AS totalAccesses;
ddata = FOREACH unique GENERATE
    group.byWho AS byWho,
    group.whatPage AS whatPage;
groupNow = GROUP ddata BY byWho;
disData = FOREACH groupNow GENERATE
    group AS byWho,
    COUNT(ddata) AS distinctPages;
join5 = JOIN adata BY byWho, disData BY byWho;
data5 = FOREACH join5 GENERATE
    adata::byWho AS byWho,
    adata::totalAccesses AS totalAccesses,
    disData::distinctPages AS distinctPages;
STORE data5 INTO '/user/cs4433/project/facebook_analytics/output/favorites.csv' USING PigStorage(',');


-- TaskF
friends = FOREACH friendsRaw GENERATE personID, myFriend;
logs = FOREACH accessRaw GENERATE byWho, whatPage;
join6a = JOIN friends BY myFriend, logs BY byWho;
fAccess = FOREACH join6a GENERATE
    friends::personID AS personID,
    friends::myFriend AS myFriend;
frAccess = DISTINCT fAccess;
join6b = JOIN friends BY (personID, myFriend) LEFT OUTER, frAccess BY (personID, myFriend);
fNot = FILTER join6b BY frAccess::personID IS NULL;
fjoin6 = JOIN fNot BY personID, pagesRaw BY personID;
data6 = FOREACH fjoin6 GENERATE
    fNot::personID AS personID,
    pagesRaw::name AS name;
STORE data6 INTO '/user/cs4433/project/facebook_analytics/output/notAccessedFriends.csv' USING PigStorage(',');


-- TaskG
refDate = ToDate('2025-02-22');
accessRawWithDate = FOREACH accessRaw GENERATE *, ToDate(accessTime) AS accessDate;
filterDate = FILTER accessRawWithDate BY DaysBetween(refDate, accessDate) > 14;
dPeople = FOREACH filterDate GENERATE byWho;
distinctPeople = DISTINCT dPeople;
join7 = JOIN distinctPeople BY byWho LEFT OUTER, pagesRaw BY personID;
data7 = FOREACH join7 GENERATE
    distinctPeople::byWho AS personID,
    pagesRaw::name AS name;
STORE data7 INTO '/user/cs4433/project/facebook_analytics/output/disconnected.csv' USING PigStorage(',');


-- TaskH
friends = GROUP friendsRaw BY personID;
data = FOREACH friends GENERATE
    group AS personID,
    COUNT(friendsRaw) AS friendCount;
total = FOREACH (GROUP data BY ALL) GENERATE SUM(data.friendCount) AS tot;
people = FOREACH (GROUP data BY ALL) GENERATE COUNT(data) AS peop;
crossData = CROSS total, people;
average = FOREACH crossData GENERATE total.tot / people.peop AS aver;
datAvg = CROSS data, average;
filter8 = FILTER datAvg BY friendCount > average.aver;
join8 = JOIN filter8 BY personID LEFT OUTER, pagesRaw BY personID;
fdata8 = FOREACH join8 GENERATE
    filter8::personID AS personID,
    pagesRaw::name AS name,
    filter8::friendCount AS friendCount;
STORE fdata8 INTO '/user/cs4433/project/facebook_analytics/output/popular.csv' USING PigStorage(',');
