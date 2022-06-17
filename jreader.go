/*
 * RJeader is a JSON parser for huge data structures.
 *
 * 2022 - JReader, by Philip Seeger (c0xc)
 */

package json_extractor

import (
    "fmt"
    "os"
    "bufio"
    "encoding/json"
    "strings"
    "io"
    "errors"
    "regexp"
    "strconv"
)

var rxMultiMatch *regexp.Regexp = regexp.MustCompile(`^(.*?)(\[(\d*)\])$`)

type resultContainer struct {
    seen []*PathRef
    skipped []*PathRef
    result map[string]string //map[string]interface{} not yet supported
    fullState bool
}

type pathNode struct {
    index int
    key string
}

type PathRef struct {
    pathL []string
    atD []pathNode
    raw string
    knownStringMatches []string
}

type schemaItem struct {
    ReqPath *PathRef
    DstKey string
}

func (self *schemaItem) IsMulti() bool {
    src := self.ReqPath.String()
    if strings.HasSuffix(src, "[]") { //TODO not optimal
        return true
    }
    return false
}

func newPathRef(pathL []string, atD map[int]*pathNode) *PathRef {
    //This creates a path reference object.
    //It references a path node in the data stream.
    //It can be represented as a string:
    //Example: .[0].List[0]
    var pathObj PathRef
    pathObj.pathL = pathL
    pathObj.atD = make([]pathNode, len(pathL))
    for i := range pathL {
        var node pathNode = *atD[i]
        pathObj.atD[i] = node
    }
    return &pathObj
}

func NewPath(pathStr string) *PathRef {
    //This creates a virtual path object from a string,
    //which could be a user-defined path in the schema.
    //We use this type of object to be able to compare it with other paths
    //without having to take the string apart each time.
    //Note that this "virtual" path object will not be a reference,
    //unlike the one returned by the other ctor.
    //Example: .[0].List[0] (absolute path)
    //Example: .[0].List[] / .[].List[] (multi path)
    var pathObj PathRef
    pathObj.raw = pathStr
    parts := strings.Split(pathStr, ".")
    for i, part := range parts {
        if i == 0 {
            //".[]..." leading point for document start
            if part != "" {
                return nil
            }
            continue
        } else {
            if part == "" {
                //Empty path part
                return nil
            }
        }

        //Structs with metadata
        node := pathNode{index: -1}
        foundArray := false
        indexPart := ""
        partMod := part

        //Add object key
        multiMatch := rxMultiMatch.FindStringSubmatch(part) //"...[]"
        if len(multiMatch) > 2 {
            foundArray = true
            partMod = multiMatch[1]
            indexPart = multiMatch[3]
        }
        if partMod != "" {
            node.key = partMod
            pathObj.pathL = append(pathObj.pathL, "OBJECT")
            pathObj.atD = append(pathObj.atD, node)
        }
        //hint: this string parser might be slightly buggy

        //Add array
        if foundArray {
            node := pathNode{index: -1}
            if indexPart != "" {
                if n, err := strconv.Atoi(indexPart); err == nil {
                    node.index = n
                } else {
                    //Not a number (in brackets)
                    return nil
                }
            }
            pathObj.pathL = append(pathObj.pathL, "ARRAY")
            pathObj.atD = append(pathObj.atD, node)
        }
    }

    return &pathObj
}

func (self *PathRef) String() string {
    var pathStr string
    pathStr = "."
    for d, l := range self.pathL {
        node := &self.atD[d]
        if l == "OBJECT" {
            //Object delimiter "."
            pathStr += "."
            //Key required
            pathStr += node.key //first one empty (highly likely)
        } else if l == "ARRAY" {
            //Object delimiter "[]"
            var indexStr string
            if node.index > -1 {
                indexStr = strconv.Itoa(node.index)
            }
            pathStr = fmt.Sprintf("%s[%s]", pathStr, indexStr)
        }
    }
    return pathStr
}

func (self *PathRef) matches(cmpPath *PathRef) bool {
    //Check for known string matches first
    //If this path object references a path in the structure being parsed,
    //this should be used to compare it with a known path from the schema.
    //So, if we're at .[2].List[7], it would match a known schema path
    //".[].List[]" (but not ".[0].List[]").
    //for _, knownStr := range self.knownStringMatches {
    //    //TODO ... rely on array, prefilled during parsing, on key...
    //    //note: this cache/comparison only works if left/self is absolute
    //    //and right/other is ... wait what
    //    if knownStr == cmpPath {
    //        return true
    //    }
    //}

    //Compare path ...
    if len(self.pathL) == 0 {
        return false
    }
    if len(self.pathL) != len(cmpPath.pathL) {
        return false
    }
    for d, l := range self.pathL {
        //Check type
        if l != cmpPath.pathL[d] {
            return false
        }
        //Check key/position
        if l == "ARRAY" {
            //note: unindexed arrays must have index = -1
            if cmpPath.atD[d].index == -1 {
                //match any index if other path has unindexed array []
            } else if self.atD[d].index != cmpPath.atD[d].index {
                return false
            }
        } else if l == "OBJECT" {
            if self.atD[d].key != cmpPath.atD[d].key {
                return false
            }
        }
    }
    return true
}
func (self *PathRef) isArray(i int) bool {
    return self.pathL[i] == "ARRAY"
}

//func (self *JReader) inArray() bool {
//    if len(self.pathL) == 0 {
//        return false
//    }
//    return self.pathL[self.depthIndex()] == "ARRAY"
//}
//
//func (self *JReader) inObject() bool {
//    if len(self.pathL) == 0 {
//        return false
//    }
//    return self.pathL[self.depthIndex()] == "OBJECT"
func (self *PathRef) hasIndex(i int) bool {
    return self.atD[i].index > -1
}

type JReader struct {
    filePath string
    file io.Reader
    json *json.Decoder
    res map[string]string
    pathL []string
    atD map[int]*pathNode
    resState resultContainer
    schema map[string]string //TODO typedef ...
    schemaItems []schemaItem
    srcPathLst []string
    shortestPathDepthInt int
    rxMultiMatch *regexp.Regexp
    reqInfoMap map[string]map[string]string
}

func NewJReader(ifile string) *JReader {
    r := &JReader{}
    r.filePath = ifile

    if ifile == "" {
        return nil
    }
    //var file *io.Reader
    if ifile == "-" {
        r.file = bufio.NewReader(os.Stdin)
    } else {
        if file, err := os.Open(ifile); err == nil {
            r.file = file
        } else {
            fmt.Printf("ERROR - failed to open file %s\n", ifile)
            os.Exit(1)
        }
    }

    r.json = json.NewDecoder(r.file)

    r.rxMultiMatch = regexp.MustCompile(`^(.*?)(\[\d+\])$`) //TODO obsolete

    r.init()

    return r
}

func (self *JReader) SetSchema(userSchema map[string]string) {
    //TODO typedef or accept alternative input format
    self.schema = userSchema
    self.schemaItems = nil
    for k, v := range userSchema {
        newItem := schemaItem{}
        newItem.ReqPath = NewPath(v)
        newItem.DstKey = k
        self.schemaItems = append(self.schemaItems, newItem)
    }

    var paths []string
    for _, p := range self.schema {
        paths = append(paths, p)
    }
    self.srcPathLst = paths

    var depth int
    for _, p := range self.srcPaths() {
        d := len(strings.Split(p, "."))
        if depth == 0 || d < depth {
            depth = d
        }
    }
    self.shortestPathDepthInt = depth

}

func (self *JReader) init() {
    self.res = make(map[string]string)
    self.pathL = nil
    self.atD = make(map[int]*pathNode)
    self.resState = resultContainer{} //res stays undefined/nil
    self.reqInfoMap = make(map[string]map[string]string) //cache
}

func (self *JReader) currentPath() *PathRef {
    path := newPathRef(self.pathL, self.atD)
    return path
}

func (self *JReader) depth() int {
    return len(self.pathL)
}

func (self *JReader) depthIndex() int {
    return self.depth() - 1
}

func (self *JReader) currentNode() *pathNode {
    node := self.atD[self.depthIndex()]
    return node
}

func (self *JReader) inArray() bool {
    if len(self.pathL) == 0 {
        return false
    }
    return self.pathL[self.depthIndex()] == "ARRAY"
}

func (self *JReader) inObject() bool {
    if len(self.pathL) == 0 {
        return false
    }
    return self.pathL[self.depthIndex()] == "OBJECT"
}

func (self *JReader) prune() {
    for k, _ := range self.atD {
        if k > self.depthIndex() {
            delete(self.atD, k)
        }
    }
}

func (self *JReader) handleEvent(ev string) {

    //ev == "doc_start": not implemented

    if ev == "object_start" {
        //New object
        self.pathL = append(self.pathL, "OBJECT")
        var d int = self.depthIndex()
        self.atD[d] = &pathNode{}
    } else if ev == "object_end" {
        //Go one level up
        self.pathL = self.pathL[:len(self.pathL)-1]
        //index++ for next array element
        if self.inArray() {
            node := self.currentNode()
            node.index += 1
        }
        //Clear position data from previous level (pop stack)
        self.prune()
    }
    if ev == "array_start" {
        //New array
        self.pathL = append(self.pathL, "ARRAY")
        //New node (path) element with index = 0
        self.atD[self.depthIndex()] = &pathNode{}
    } else if ev == "array_end" {
        //Go one level up
        self.pathL = self.pathL[:len(self.pathL)-1]
        //index++ for next array element
        if self.inArray() {
            node := self.currentNode()
            node.index += 1
        }
        //Clear position data from previous level (pop stack)
        self.prune()
    }

}

func (self *JReader) Read() (map[string]string, error) {
    var result map[string]string
    for result == nil {
        t, err := self.json.Token()
        if err == io.EOF {
            return nil, io.EOF
        } else if err != nil {
            return nil, errors.New("read error")
        }

        //Event
        var ev string
        var value string //interface{}
        switch v := t.(type) {
            case json.Delim:
            t := v.String()
            if t == "{" {
                ev = "object_start"
            } else if t == "}" {
                ev = "object_end"
            } else if t == "[" {
                ev = "array_start"
            } else if t == "]" {
                ev = "array_end"
            }
            case string:
            if self.inObject() {
                node := self.currentNode()
                if len(node.key) == 0 { //we don't allow blank keys
                    ev = "key"
                } else {
                    ev = "value"
                }
            } else {
                ev = "value"
            }
            value = v
            //default:
            //value = fmt.Sprintf("%v", t)
            //other types not implemented
        }

        //Update stack
        self.handleEvent(ev)
        self.checkClearRes()

        //Node key (object)
        if ev == "key" {
            node := self.currentNode()
            node.key = value
        }
        //Node element, value
        if ev == "value" {
            if r := self.setValue(value); r != nil {
                result = r
            }
            if self.inArray() {
                node := self.currentNode()
                node.index += 1
            }
            if self.inObject() {
                self.currentNode().key = ""
            }
        }

    }

    return result, nil
}

func (self *JReader) checkClearRes() {
    //Check if we're above all requested paths
    //Once we are, the current result object must either be empty or full;
    //new matches belong to the next result object.
    d0 := self.shortestPathDepth() //shortest req path, i.e., boundary
    //d0 has one additional item for doc_start lol
    if self.depth() >= d0 - 1 {
        return //we're within the search area for a single object
    }
    path := self.currentPath()

    //Nothing to do if current result object empty
    if len(self.resState.result) == 0 {
        return
    }

    //Check if current result object incomplete
    if !self.isFull() {
        //Incomplete result, not all requested keys found
        //TODO if allow_incomplete_result: send/yield?
        fmt.Printf("ERROR - incomplete result object at %s; seen only: %s\n", path.String(), self.resState.seen)
        os.Exit(3)
    }

    //Check for skipped elements
    if len(self.resState.skipped) > 0 {
        fmt.Printf("ERROR - skipped elements at at %s (input out of order?)\n", path.String())
        os.Exit(3)
    }

    //Reset result after we've left the req/search area
    //(full result has already been yielded in setValue)
    self.resState = resultContainer{}
    self.checkFull()

}

func (self *JReader) srcPaths() []string {
    return self.srcPathLst
}

func (self *JReader) shortestPathDepth() int {
    return self.shortestPathDepthInt
}

func (self *JReader) pathMatches(reqPath, cmpPath string) bool {
    reqPathParts := strings.Split(reqPath, ".") //requested path or pattern
    pathModParts := strings.Split(cmpPath, ".")
    if len(reqPathParts) != len(pathModParts) {
        return false
    }
    var cmpPathMod string
    j := -1
    for i, part := range pathModParts {
        j += 1
        if j > 0 {
            cmpPathMod += "."
        }
        partMod := part
        reqPart := reqPathParts[i]
        if len(reqPart) > 1 && reqPart[len(reqPart)-2:] == "[]" {
            multiMatch := self.rxMultiMatch.FindStringSubmatch(partMod)
            if len(multiMatch) > 2 {
                partMod = multiMatch[1] + "[]"
            }
        }
        cmpPathMod += partMod
    }

    return reqPath == cmpPathMod
}

func (self *JReader) checkFull() bool {
    foundAllKeys := true
    for _, src := range self.srcPaths() {
        srcSeen := false
        for _, v := range self.resState.seen {
            if v.String() == src {
                srcSeen = true
            }
        }
        if !srcSeen {
            foundAllKeys = false
            break
        }
    }
    self.resState.fullState = foundAllKeys
    return self.isFull()
}

func (self *JReader) isFull() bool {
    return self.resState.fullState
}

func (self *JReader) reqPath(cmpPath *PathRef) *schemaItem {
    //Return info about requested path or nothing if path not requested
    //cmpPath references absolute path like .[0].List[8]
    for _, schemaItem := range self.schemaItems {
        //NOTE when comparing absolute vs dynamic path, right/other = dynamic
        if cmpPath.matches(schemaItem.ReqPath) {
            return &schemaItem
        }
    }

    return nil
}
//TODO Check for partial match while building path

func (self *JReader) isPathRequested(path *PathRef) bool {
    return self.reqPath(path) != nil
}

func (self *JReader) setValue(value string) map[string]string {
    //Handle value, update result object...

    //Skip if current element path not on list of requested paths
    path := self.currentPath()
    req := self.reqPath(path)
    if !self.isPathRequested(path) {
        return nil
    }
    reqSrc := req.ReqPath
    dstKey := req.DstKey //TODO method
    isMulti := req.IsMulti()

    //Prepare result object, "seen" list for found keys
    //Remember path to beginning of result object
    markedSkipped := false
    resContainer := &self.resState
    if resContainer.result == nil {
        resContainer.result = make(map[string]string)
        //resContainer.p0 = path //...
    }
    result := resContainer.result
    //Check for collision (already found), fatal only for absolute path
    //NOTE we might skip items if collisions are not recognized properly
    if _, ok := result[dstKey]; ok {
        if isMulti {
            //skip element at: path
            resContainer.skipped = append(resContainer.skipped, path)
            markedSkipped = true
        } else {
            //Regular req key already found - collision
            //raise Exception("collision before full at %s (bad order?)", path
            fmt.Printf("ERROR - collision before full at %s (bad order?)\n", path.String())
            os.Exit(3)
        }
    }

    //Add value to result object, add path to "seen" list for result
    result[dstKey] = value
    resContainer.seen = append(resContainer.seen, reqSrc)
    //self.checkFull()
    if len(resContainer.seen) == len(self.srcPaths()) {
        self.resState.fullState = true
    }

    //Check if result object is complete
    if self.isFull() {
        if markedSkipped {
            resContainer.skipped = resContainer.skipped[:len(resContainer.skipped)-1]
        }
        //Yield result
        return result
        //NOT clearing and resetting here - there could be more
        //(more matches with collected values within req area)
        //see also (for clear/yield): checkClearRes()
    }

    return nil
}

