addEvent(window, "load", function () {

    // ------------------
    // DEBUG - Put something in the content area just for testing
    // This could be replaced with some kind of introduction text,
    // "Welcome to ATK!", copyright info, etc.

    var xmlhttp = new XMLHttpRequest();
    // a convenient sample text file
    var docspath = "../adventures-of-sherlock-holmes.txt";  //DEBUG

    alert("Opening file "+ docspath);                //DEBUG
    xmlhttp.open("GET", docspath, false);
    xmlhttp.send();

    var xmlDoc = xmlhttp.responseText;
    // now insert the entire contents of the other url on the same server 
    // inside a particular div (the rest of the page)

    alert("Loaded "+ xmlDoc.length + "chars");      //DEBUG
    xmlDoc = xmlDoc.replace(/(\r\n\r\n|\n\n)/g, '<BR><BR>');    //DEBUG
    
    document.getElementById("content").innerHTML = xmlDoc;
    // ------------------

    
    var menucontainer = document.getElementById("versionmenu-container");

    if (menucontainer) {
	addEvent(menucontainer, "click", function (event) {

	    var target = event.target || event.srcElement; // Cross-browser (IE < 8) fix
	    
	    // Figure out the version given the id string of the
	    // actual element clicked.
	    var version_string = target.id;

	    alert("Click on id: " + target.id);   // DEBUG 

	    // This regex matches version numbers of the forms X.Y or
	    // X.Y.Z, where X/Y/Z are any number of digits, but not
	    // just X alone.
	    var version_re = /((\d+)\.(\d+)\.?(\d+)?)/;
	    var version_arr = version_string.match(version_re);
	    if (version_arr === null || version_arr[0] === undefined ||
		version_arr[1] === undefined || version_arr[2] === undefined ||
		version_arr[3] === undefined) {
		// Something went wrong.  Probably the click was
		// inside the "menucontainer" div but outside any of
		// the version menu links.  So just let the click
		// bubble up.

		alert("Click event went nowhere");   // DEBUG 

		return;
	    }
	    // We're handling the event, so stop it from bubbling up
	    // further in the DOM.  Cross-browser-compatible code
	    // (IE < 8 is the problem) from
	    // <http://javascript.info/tutorial/bubbling-and-capturing>
	    event = event || window.event;
	    if (event.stopPropagation) {
		// W3C standard variant
		event.stopPropagation();
	    } else {
		// IE variant
		event.cancelBubble = true;
	    }
	    
	    // In case we need the parts of the version number.
	    var major = version_arr[2];
	    var minor = version_arr[3];
	    var patch = version_arr[4];
	    
	    // Use AJAX to load the home page of the appropriate
	    // version of the docs into a variable.
	    var xmlhttp = new XMLHttpRequest();
	    var docspath = '../ta-atk/index.html';  //DEBUG
	    // DEBUG: Uncomment the next line once we have different versions
	    // of the index.html for the different releases, or change
	    // the string to point to a different directory by release number,
	    // or whatever.
	    // I.e., something like
	    //	    var docspath = version_arr[1] + '/index.html'
	    // if the URL is 'trustedanalytics.github.io/atk/0.4.0'

	    alert("Opening " + docspath);   // DEBUG 
	    xmlhttp.open("GET", docspath, false);
	    xmlhttp.send();

	    var xmlDoc = xmlhttp.responseXML;

	    // Now insert the entire contents of the other URL inside
	    // the correct div.
	    document.getElementById("content").innerHTML = xmlDoc;
        }, false /* bubble, don't capture */);
    }
});





