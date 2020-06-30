package main

type set map[string]struct{}

func newSet() set {
	return make(set)
}

func (s set) has(k string) bool {
	_, has := s[k]
	return has
}

func (s set) add(k string) {
	s[k] = struct{}{}
}

func (s set) copy() set {
	c := newSet()
	for k := range s {
		c.add(k)
	}
	return c
}

func (s set) keys() []string {
	var ks []string
	for k := range s {
		ks = append(ks, k)
	}
	return ks
}

func (s set) diff(s2 set) set {
	d := newSet()
	for k := range s {
		if !s2.has(k) {
			d.add(k)
		}
	}
	return d
}

func (s set) isSubSet(s2 set) bool {
	for k := range s {
		if !s2.has(k) {
			return false
		}
	}
	return true
}
