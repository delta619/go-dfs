package utils

type Set map[string]bool

func (s Set) Add(str string) {
	s[str] = true
}

func (s Set) Remove(str string) {
	delete(s, str)
}

func (s Set) Contains(str string) bool {
	_, exists := s[str]
	return exists
}

func (s Set) Size() int {
	return len(s)
}

func GetAllNodesFromChunkMeta(chunkMeta map[string]interface{}) []string {
	nodes := make([]string, 0)

	nodes = append(nodes, chunkMeta["PrimaryNode"].(string))
	nodes = append(nodes, chunkMeta["SecondaryNodes"].([]string)...)
	return nodes
}

func RemoveElementFromList(slice []string, element string) []string {

	result := make([]string, 0)

	for _, sliceElement := range slice {
		if sliceElement != element {
			result = append(result, sliceElement)
		}
	}
	return result
}
