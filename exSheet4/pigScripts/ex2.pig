REGISTER $rdfStorageFolder/RDFStorage.jar;

-- loads social graph
indata = LOAD '$input' USING RDFStorage() AS (s, p, o) ;

-- checks if a given parameter for user is of type sib:User
user = FILTER indata BY s == '$userId' AND p == 'a' AND o == 'sib:User' ;

-- gets all friends of the user $userId
userFriends = FILTER indata BY p == 'foaf:knows' AND s == user.$0 ;  

-- gets all photo albums by checking their type (sioct:ImageGallery)
photoAlbums = FILTER indata BY p == 'rdf:type' AND o == 'sioct:ImageGallery' ;

-- gets all creations of users
usersCreations = FILTER indata BY p == 'sioc:creator_of' ;

-- gets all items that are created by friends of the user ($user)
userFriendsCreationsJoin = JOIN usersCreations BY s, userFriends BY o ;
userFriendsCreations = FOREACH userFriendsCreationsJoin GENERATE $0, $1, $2 ;

-- gets all albums that are created by friends of the user ($user)
userFriendsAlbumsJoin = JOIN userFriendsCreations BY o, photoAlbums BY s ;
userFriendsAlbums = FOREACH userFriendsAlbumsJoin GENERATE $3, $4, $5 ;

-- gets all stamtements with predicate sioc:container_of
containters = FILTER indata BY p == 'sioc:container_of' ;

-- gets all photos by checking their type (sib:Photo)
photos = FILTER indata BY p == 'a' AND o == 'sib:Photo' ;

-- get only container_of photos (photo_album)
photoAlbumItemsJoin = JOIN containters BY o, photos BY s ;
photoAlbumItems = FOREACH photoAlbumItemsJoin GENERATE $0, $1, $2 ;

-- gets only those photos which are created by friends of the user
userFriendsPhotoAlbumItemsJoin = JOIN photoAlbumItems BY s, userFriendsAlbums BY s ;
userFriendsPhotoAlbumItems = FOREACH userFriendsPhotoAlbumItemsJoin GENERATE $0, $1, $2 ;

-- gets all photos the user is tagged in 
userTags = FILTER indata  BY p == 'sib:usertag' AND o == '$userId' ;

-- gets all photos which are created by user's friends and the user is tagged in
userTagsFriendPhotosJoin = JOIN userFriendsPhotoAlbumItems BY o, userTags BY s ; 
userTagsFriendPhotos = FOREACH userTagsFriendPhotosJoin GENERATE $3, $4, $5 ;

-- stores the result into output file
STORE userTagsFriendPhotos INTO '$output' ;