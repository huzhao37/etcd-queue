/**
 * @Author: hiram
 * @Date: 2021/3/18 13:43
 */
package etcdq

var Exists = struct{}{}

type set interface {
	Add(items interface{})
	Contains(item interface{}) bool
	Size() int
	Clear()
}
type Set struct {
	m map[interface{}]struct{}
}

var _ set = new(Set)

func (s *Set) Add(item interface{}) {
	if s.Contains(item) {
		return
	}
	s.m[item] = Exists
}
func (s *Set) Contains(item interface{}) bool {
	_, ok := s.m[item]
	return ok
}
func (s *Set) Size() int {
	return len(s.m)
}
func (s *Set) Clear() {
	s.m = make(map[interface{}]struct{})
}

func NewSet() Set {
	s := Set{}
	s.m = make(map[interface{}]struct{})
	return s
}
