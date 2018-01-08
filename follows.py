def func(person, listOfWhoTheyFollow):
	rdd = []
	rdd.append( ((person,"follows"), listOfWhoTheyFollow) )
	for friend in listOfWhoTheyFollow:
		rdd.append(((friend, 'followed_by'), person))
	return rdd



def recommend(key,val) :
	recommlist = []
	
	if(len(val) == 2):
		listOfWhoTheyFollow = val[0][1]
		listOfPeopleThatFollowThem = val[1][1]
		for followed in listOfWhoTheyFollow:
			for follower in listOfPeopleThatFollowThem:
			
				# Do not include if already following the person:
				if follower not in listOfWhoTheyFollow:
					recommlist.append( (follower,followed) )
	
	return recommlist