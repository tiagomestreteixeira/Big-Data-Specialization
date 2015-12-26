def split_fileA(line):
	line_splited = line.split(",")
	return (line_splited[0],(int)(line_splited[1]))

def split_fileB(line):
    initialsplit = line.split()
    nondatesplit = (initialsplit[1]).split(",")
    return (nondatesplit[0],initialsplit[0]+' '+ nondatesplit[1])
