package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie/util"
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

// All will return true if all callbacks return true. It follows the same logic
// as the all() function in Python.
//
// If the list is empty then true is always returned.
func (ss WsConnections) All(fn func(value *WsConn) bool) bool {
	for _, value := range ss {
		if !fn(value) {
			return false
		}
	}
	return true
}

// Any will return true if any callbacks return true. It follows the same logic
// as the any() function in Python.
//
// If the list is empty then false is always returned.
func (ss WsConnections) Any(fn func(value *WsConn) bool) bool {
	for _, value := range ss {
		if fn(value) {
			return true
		}
	}
	return false
}

// Append will return a new slice with the elements appended to the end.
//
// It is acceptable to provide zero arguments.
func (ss WsConnections) Append(elements ...*WsConn) WsConnections {
	// Copy ss, to make sure no memory is overlapping between input and
	// output. See issue #97.
	result := append(WsConnections{}, ss...)
	result = append(result, elements...)
	return result
}

// Bottom will return n elements from bottom
//
// that means that elements is taken from the end of the slice
// for this [1,2,3] slice with n == 2 will be returned [3,2]
// if the slice has less elements then n that'll return all elements
// if n < 0 it'll return empty slice.
func (ss WsConnections) Bottom(n int) (top WsConnections) {
	var lastIndex = len(ss) - 1
	for i := lastIndex; i > -1 && n > 0; i-- {
		top = append(top, ss[i])
		n--
	}
	return
}

// Contains returns true if the element exists in the slice.
//
// When using slices of pointers it will only compare by address, not value.
func (ss WsConnections) Contains(lookingFor *WsConn) bool {
	for _, s := range ss {
		if lookingFor == s {
			return true
		}
	}
	return false
}

// Diff returns the elements that needs to be added or removed from the first
// slice to have the same elements in the second slice.
//
// The order of elements is not taken into consideration, so the slices are
// treated sets that allow duplicate items.
//
// The added and removed returned may be blank respectively, or contain upto as
// many elements that exists in the largest slice.
func (ss WsConnections) Diff(against WsConnections) (added, removed WsConnections) {
	// This is probably not the best way to do it. We do an O(n^2) between the
	// slices to see which items are missing in each direction.
	diffOneWay := func(ss1, ss2raw WsConnections) (result WsConnections) {
		for _, s := range ss1 {
			found := false
			for _, element := range ss2raw {
				if s == element {
					found = true
					break
				}
			}
			if !found {
				result = append(result, s)
			}
		}
		return
	}
	removed = diffOneWay(ss, against)
	added = diffOneWay(against, ss)
	return
}

// DropTop will return the rest slice after dropping the top n elements
// if the slice has less elements then n that'll return empty slice
// if n < 0 it'll return empty slice.
func (ss WsConnections) DropTop(n int) (drop WsConnections) {
	if n < 0 || n >= len(ss) {
		return
	}
	// Copy ss, to make sure no memory is overlapping between input and
	// output. See issue #145.
	drop = make([]*WsConn, len(ss)-n)
	copy(drop, ss[n:])
	return
}

// Each is more condensed version of Transform that allows an action to happen
// on each elements and pass the original slice on.
//
//	cars.Each(func (car *Car) {
//	    fmt.Printf("Car color is: %s\n", car.Color)
//	})
//
// Pie will not ensure immutability on items passed in so they can be
// manipulated, if you choose to do it this way, for example:
//
//	// Set all car colors to Red.
//	cars.Each(func (car *Car) {
//	    car.Color = "Red"
//	})
func (ss WsConnections) Each(fn func(*WsConn)) WsConnections {
	for _, s := range ss {
		fn(s)
	}
	return ss
}

// Equals compare elements from the start to the end,
//
// if they are the same is considered the slices are equal if all elements are the same is considered the slices are equal
// if each slice == nil is considered that they're equal
//
// if element realizes Equals interface it uses that method, in other way uses default compare
func (ss WsConnections) Equals(rhs WsConnections) bool {
	if len(ss) != len(rhs) {
		return false
	}
	for i := range ss {
		if !(ss[i] == rhs[i]) {
			return false
		}
	}
	return true
}

// Extend will return a new slice with the slices of elements appended to the
// end.
//
// It is acceptable to provide zero arguments.
func (ss WsConnections) Extend(slices ...WsConnections) (ss2 WsConnections) {
	ss2 = ss
	for _, slice := range slices {
		ss2 = ss2.Append(slice...)
	}
	return ss2
}

// Filter will return a new slice containing only the elements that return
// true from the condition. The returned slice may contain zero elements (nil).
//
// FilterNot works in the opposite way of Filter.
func (ss WsConnections) Filter(condition func(*WsConn) bool) (ss2 WsConnections) {
	for _, s := range ss {
		if condition(s) {
			ss2 = append(ss2, s)
		}
	}
	return
}

// FilterNot works the same as Filter, with a negated condition. That is, it will
// return a new slice only containing the elements that returned false from the
// condition. The returned slice may contain zero elements (nil).
func (ss WsConnections) FilterNot(condition func(*WsConn) bool) (ss2 WsConnections) {
	for _, s := range ss {
		if !condition(s) {
			ss2 = append(ss2, s)
		}
	}
	return
}

// FindFirstUsing will return the index of the first element when the callback returns true or -1 if no element is found.
// It follows the same logic as the findIndex() function in Javascript.
//
// If the list is empty then -1 is always returned.
func (ss WsConnections) FindFirstUsing(fn func(value *WsConn) bool) int {
	for idx, value := range ss {
		if fn(value) {
			return idx
		}
	}
	return -1
}

// First returns the first element, or zero. Also see FirstOr().
func (ss WsConnections) First() *WsConn {
	return ss.FirstOr(nil)
}

// FirstOr returns the first element or a default value if there are no
// elements.
func (ss WsConnections) FirstOr(defaultValue *WsConn) *WsConn {
	if len(ss) == 0 {
		return defaultValue
	}
	return ss[0]
}

// Float64s transforms each element to a float64.
func (ss WsConnections) Float64s() pie.Float64s {
	l := len(ss)
	// Avoid the allocation.
	if l == 0 {
		return nil
	}
	result := make(pie.Float64s, l)
	for i := 0; i < l; i++ {
		mightBeString := ss[i]
		result[i], _ = strconv.ParseFloat(fmt.Sprintf("%v", mightBeString), 64)
	}
	return result
}

// Insert a value at an index
func (ss WsConnections) Insert(index int, values ...*WsConn) WsConnections {
	if index >= ss.Len() {
		return WsConnections.Extend(ss, WsConnections(values))
	}
	return WsConnections.Extend(ss[:index], WsConnections(values), ss[index:])
}

// Ints transforms each element to an integer.
func (ss WsConnections) Ints() pie.Ints {
	l := len(ss)
	// Avoid the allocation.
	if l == 0 {
		return nil
	}
	result := make(pie.Ints, l)
	for i := 0; i < l; i++ {
		mightBeString := ss[i]
		f, _ := strconv.ParseFloat(fmt.Sprintf("%v", mightBeString), 64)
		result[i] = int(f)
	}
	return result
}

// Join returns a string from joining each of the elements.
func (ss WsConnections) Join(glue string) (s string) {
	var slice interface{} = []*WsConn(ss)
	if y, ok := slice.([]string); ok {
		// The stdlib is efficient for type []string
		return strings.Join(y, glue)
	} else {
		// General case
		parts := make([]string, len(ss))
		for i, element := range ss {
			mightBeString := element
			parts[i] = fmt.Sprintf("%v", mightBeString)
		}
		return strings.Join(parts, glue)
	}
}

// JSONBytes returns the JSON encoded array as bytes.
//
// One important thing to note is that it will treat a nil slice as an empty
// slice to ensure that the JSON value return is always an array.
func (ss WsConnections) JSONBytes() []byte {
	if ss == nil {
		return []byte("[]")
	}
	// An error should not be possible.
	data, _ := json.Marshal(ss)
	return data
}

// JSONBytesIndent returns the JSON encoded array as bytes with indent applied.
//
// One important thing to note is that it will treat a nil slice as an empty
// slice to ensure that the JSON value return is always an array. See
// json.MarshalIndent for details.
func (ss WsConnections) JSONBytesIndent(prefix, indent string) []byte {
	if ss == nil {
		return []byte("[]")
	}
	// An error should not be possible.
	data, _ := json.MarshalIndent(ss, prefix, indent)
	return data
}

// JSONString returns the JSON encoded array as a string.
//
// One important thing to note is that it will treat a nil slice as an empty
// slice to ensure that the JSON value return is always an array.
func (ss WsConnections) JSONString() string {
	if ss == nil {
		return "[]"
	}
	// An error should not be possible.
	data, _ := json.Marshal(ss)
	return string(data)
}

// JSONStringIndent returns the JSON encoded array as a string with indent applied.
//
// One important thing to note is that it will treat a nil slice as an empty
// slice to ensure that the JSON value return is always an array. See
// json.MarshalIndent for details.
func (ss WsConnections) JSONStringIndent(prefix, indent string) string {
	if ss == nil {
		return "[]"
	}
	// An error should not be possible.
	data, _ := json.MarshalIndent(ss, prefix, indent)
	return string(data)
}

// Last returns the last element, or zero. Also see LastOr().
func (ss WsConnections) Last() *WsConn {
	return ss.LastOr(nil)
}

// LastOr returns the last element or a default value if there are no elements.
func (ss WsConnections) LastOr(defaultValue *WsConn) *WsConn {
	if len(ss) == 0 {
		return defaultValue
	}
	return ss[len(ss)-1]
}

// Len returns the number of elements.
func (ss WsConnections) Len() int {
	return len(ss)
}

// Map will return a new slice where each element has been mapped (transformed).
// The number of elements returned will always be the same as the input.
//
// Be careful when using this with slices of pointers. If you modify the input
// value it will affect the original slice. Be sure to return a new allocated
// object or deep copy the existing one.
func (ss WsConnections) Map(fn func(*WsConn) *WsConn) (ss2 WsConnections) {
	if ss == nil {
		return nil
	}
	ss2 = make([]*WsConn, len(ss))
	for i, s := range ss {
		ss2[i] = fn(s)
	}
	return
}

// Mode returns a new slice containing the most frequently occuring values.
//
// The number of items returned may be the same as the input or less. It will
// never return zero items unless the input slice has zero items.
func (ss WsConnections) Mode() WsConnections {
	if len(ss) == 0 {
		return nil
	}
	values := make(map[*WsConn]int)
	for _, s := range ss {
		values[s]++
	}
	var maxFrequency int
	for _, v := range values {
		if v > maxFrequency {
			maxFrequency = v
		}
	}
	var maxValues WsConnections
	for k, v := range values {
		if v == maxFrequency {
			maxValues = append(maxValues, k)
		}
	}
	return maxValues
}

// Pop the first element of the slice
//
// Usage Example:
//
//	type knownGreetings []string
//	greetings := knownGreetings{"ciao", "hello", "hola"}
//	for greeting := greetings.Pop(); greeting != nil; greeting = greetings.Pop() {
//	    fmt.Println(*greeting)
//	}
func (ss *WsConnections) Pop() (popped **WsConn) {
	if len(*ss) == 0 {
		return
	}
	popped = &(*ss)[0]
	*ss = (*ss)[1:]
	return
}

// Random returns a random element by your rand.Source, or zero
func (ss WsConnections) Random(source rand.Source) *WsConn {
	n := len(ss)
	// Avoid the extra allocation.
	if n < 1 {
		return nil
	}
	if n < 2 {
		return ss[0]
	}
	rnd := rand.New(source)
	i := rnd.Intn(n)
	return ss[i]
}

// Reverse returns a new copy of the slice with the elements ordered in reverse.
// This is useful when combined with Sort to get a descending sort order:
//
//	ss.Sort().Reverse()
func (ss WsConnections) Reverse() WsConnections {
	// Avoid the allocation. If there is one element or less it is already
	// reversed.
	if len(ss) < 2 {
		return ss
	}
	sorted := make([]*WsConn, len(ss))
	for i := 0; i < len(ss); i++ {
		sorted[i] = ss[len(ss)-i-1]
	}
	return sorted
}

// Send sends elements to channel
// in normal act it sends all elements but if func canceled it can be less
//
// it locks execution of gorutine
// it doesn't close channel after work
// returns sended elements if len(this) != len(old) considered func was canceled
func (ss WsConnections) Send(ctx context.Context, ch chan<- *WsConn) WsConnections {
	for i, s := range ss {
		select {
		case <-ctx.Done():
			return ss[:i]
		default:
			ch <- s
		}
	}
	return ss
}

// SequenceUsing generates slice in range using creator function
//
// There are 3 variations to generate:
//  1. [0, n).
//  2. [min, max).
//  3. [min, max) with step.
//
// if len(params) == 1 considered that will be returned slice between 0 and n,
// where n is the first param, [0, n).
// if len(params) == 2 considered that will be returned slice between min and max,
// where min is the first param, max is the second, [min, max).
// if len(params) > 2 considered that will be returned slice between min and max with step,
// where min is the first param, max is the second, step is the third one, [min, max) with step,
// others params will be ignored
func (ss WsConnections) SequenceUsing(creator func(int) *WsConn, params ...int) WsConnections {
	var seq = func(min, max, step int) (seq WsConnections) {
		lenght := int(util.Round(float64(max-min) / float64(step)))
		if lenght < 1 {
			return
		}
		seq = make(WsConnections, lenght)
		for i := 0; i < lenght; min += step {
			seq[i] = creator(min)
			i++
		}
		return seq
	}
	if len(params) > 2 {
		return seq(params[0], params[1], params[2])
	} else if len(params) == 2 {
		return seq(params[0], params[1], 1)
	} else if len(params) == 1 {
		return seq(0, params[0], 1)
	} else {
		return nil
	}
}

// Shift will return two values: the shifted value and the rest slice.
func (ss WsConnections) Shift() (*WsConn, WsConnections) {
	return ss.First(), ss.DropTop(1)
}

// Shuffle returns shuffled slice by your rand.Source
func (ss WsConnections) Shuffle(source rand.Source) WsConnections {
	n := len(ss)
	// Avoid the extra allocation.
	if n < 2 {
		return ss
	}
	// go 1.10+ provides rnd.Shuffle. However, to support older versions we copy
	// the algorithm directly from the go source: src/math/rand/rand.go below,
	// with some adjustments:
	shuffled := make([]*WsConn, n)
	copy(shuffled, ss)
	rnd := rand.New(source)
	util.Shuffle(rnd, n, func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled
}

// SortStableUsing works similar to sort.SliceStable. However, unlike sort.SliceStable the
// slice returned will be reallocated as to not modify the input slice.
func (ss WsConnections) SortStableUsing(less func(a, b *WsConn) bool) WsConnections {
	// Avoid the allocation. If there is one element or less it is already
	// sorted.
	if len(ss) < 2 {
		return ss
	}
	sorted := make(WsConnections, len(ss))
	copy(sorted, ss)
	sort.SliceStable(sorted, func(i, j int) bool {
		return less(sorted[i], sorted[j])
	})
	return sorted
}

// SortUsing works similar to sort.Slice. However, unlike sort.Slice the
// slice returned will be reallocated as to not modify the input slice.
func (ss WsConnections) SortUsing(less func(a, b *WsConn) bool) WsConnections {
	// Avoid the allocation. If there is one element or less it is already
	// sorted.
	if len(ss) < 2 {
		return ss
	}
	sorted := make(WsConnections, len(ss))
	copy(sorted, ss)
	sort.Slice(sorted, func(i, j int) bool {
		return less(sorted[i], sorted[j])
	})
	return sorted
}

// Strings transforms each element to a string.
//
// If the element type implements fmt.Stringer it will be used. Otherwise it
// will fallback to the result of:
//
//	fmt.Sprintf("%v")
func (ss WsConnections) Strings() pie.Strings {
	l := len(ss)
	// Avoid the allocation.
	if l == 0 {
		return nil
	}
	result := make(pie.Strings, l)
	for i := 0; i < l; i++ {
		mightBeString := ss[i]
		result[i] = fmt.Sprintf("%v", mightBeString)
	}
	return result
}

// SubSlice will return the subSlice from start to end(excluded)
//
// Condition 1: If start < 0 or end < 0, nil is returned.
// Condition 2: If start >= end, nil is returned.
// Condition 3: Return all elements that exist in the range provided,
// if start or end is out of bounds, zero items will be placed.
func (ss WsConnections) SubSlice(start int, end int) (subSlice WsConnections) {
	if start < 0 || end < 0 {
		return
	}
	if start >= end {
		return
	}
	length := ss.Len()
	if start < length {
		if end <= length {
			subSlice = ss[start:end]
		} else {
			zeroArray := make([]*WsConn, end-length)
			subSlice = ss[start:length].Append(zeroArray[:]...)
		}
	} else {
		zeroArray := make([]*WsConn, end-start)
		subSlice = zeroArray[:]
	}
	return
}

// Top will return n elements from head of the slice
// if the slice has less elements then n that'll return all elements
// if n < 0 it'll return empty slice.
func (ss WsConnections) Top(n int) (top WsConnections) {
	for i := 0; i < len(ss) && n > 0; i++ {
		top = append(top, ss[i])
		n--
	}
	return
}

// StringsUsing transforms each element to a string.
func (ss WsConnections) StringsUsing(transform func(*WsConn) string) pie.Strings {
	l := len(ss)
	// Avoid the allocation.
	if l == 0 {
		return nil
	}
	result := make(pie.Strings, l)
	for i := 0; i < l; i++ {
		result[i] = transform(ss[i])
	}
	return result
}

// Unshift adds one or more elements to the beginning of the slice
// and returns the new slice.
func (ss WsConnections) Unshift(elements ...*WsConn) (unshift WsConnections) {
	unshift = append(WsConnections{}, elements...)
	unshift = append(unshift, ss...)
	return
}
