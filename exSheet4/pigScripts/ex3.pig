REGISTER $rdfStorageFolder/RDFStorage.jar ;

-- loads social graph
indata = LOAD '$input' USING RDFStorage() AS (s, p, o) ;

-- checks if a given parameter for user is of type sib:User
user = FILTER indata BY s == '$userId' AND p == 'a' AND o == 'sib:User' ;

-- gets all friends of the user $userId
userFriends = FILTER indata BY p == 'foaf:knows' AND s == user.$0 ;  

-- gets all photos by checking their type (sib:Photo)
photos = FILTER indata BY p == 'a' AND o == 'sib:Photo' ;

-- gets all items users are tagged in
usersTags = FILTER indata  BY p == 'sib:usertag' ;

-- gets all photos from all items users are tagged in
usersTagsOnPhotosJoin = JOIN photos BY s, usersTags BY s ;
usersTagsOnPhotos = FOREACH usersTagsOnPhotosJoin GENERATE $3, $4, $5 ;

-- from all photos' tags gets only these of user's friends
userFriendsPhotoTagsJoin = JOIN usersTagsOnPhotos BY o, userFriends BY o ;
userFriendsPhotoTags = FOREACH userFriendsPhotoTagsJoin GENERATE $0, $1, $2 ;

-- gets all item the user is tagged in
userTags = FILTER indata  BY p == 'sib:usertag' AND o == '$userId' ;

-- gets only photos from the above set
userPhotoTagsJoin = JOIN userTags BY s, photos BY s ;
userPhotoTags = FOREACH userPhotoTagsJoin GENERATE $0, $1, $2 ;

-- joins photos in which the user is tagged and these on which its friends are tagged
-- the result will be <photoId sib:userTag userIdParameter><samePhotoId sib:userTag friendOfUserIdParameter>
commonPhotoTags = JOIN userPhotoTags BY s, userFriendsPhotoTags BY s ;

-- group them by <user id, its friend id>
groupByPair = GROUP commonPhotoTags BY ($2, $5) ;

-- for each pair counts the number of common photos on which both are tagged (user and its friend)
commonPhotosCount = FOREACH groupByPair GENERATE group, COUNT($1) AS photosCount ;

-- orders friends by number of common photos
orderedFriendsByCommonPhotosCount = ORDER commonPhotosCount BY photosCount DESC ;

-- gets the 10 close friends of the user
tenCloseFriends = LIMIT orderedFriendsByCommonPhotosCount 10 ;

-- stores the result into the output file 
STORE tenCloseFriends INTO '$output' ;