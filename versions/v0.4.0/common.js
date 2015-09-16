//////////////////////////////////////////////////////////////////////////////////////////////
// see http://stackoverflow.com/questions/6348494/addeventlistener-vs-onclick		     //
// and http://javascript.info/tutorial/introduction-browser-events#assigning-event-handlers //
//////////////////////////////////////////////////////////////////////////////////////////////


if (document.addEventListener) {
    var addEvent = function(elem, type, handler) {
        elem.addEventListener(type, handler, false);
    };
    var removeEvent = function(elem, type, handler) {
        elem.removeEventListener(type, handler, false);
    };
} else {
    var addEvent = function(elem, type, handler) {
        elem.attachEvent("on" + type, handler);
    };
    var removeEvent = function(elem, type, handler) {
        elem.detachEvent("on" + type, handler);
    };
}


