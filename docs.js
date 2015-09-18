var VERSIONS = {};
$(document).ready(function(){
try{
  $.ajax({
  url: "https://raw.githubusercontent.com/trustedanalytics/atk/gh-pages/versions.json",
  dataType:"json"
  })
  .success(function(data, jqXHR){
    console.log(data);
    VERSIONS = data.VERSIONS;
    console.log(VERSIONS);
    updateIframeSrc(data.DEFAULT_VERSION);
    appendMenu(makeMenu(data.DEFAULT_VERSION));
  });
}catch(err){}
});

function updateIframeSrc(selected){
  $("#doc-content").attr("src", "versions/"+selected+"/");
}

function bindClick(){
  $(".doc").unbind("click");
  $(".doc").click(function() {
    updateIframeSrc(this.id);
    appendMenu(makeMenu(this.id));
  });
}

function makeMenu(selected){
  var list = '<li id="'+selected+'" class="top"><a class="selected" id="'+selected+'" >'+ VERSIONS[selected]["type"]+': '+selected+'</a>' +
             '<ul class="specific-version"> ';
  $.each( VERSIONS, function( index, value){
    if(selected != index){
      list += '<li id="'+index+'" class="bottom"><a class="doc" id="'+index+'" >'+ VERSIONS[index]["type"]+': '+index+'</a></li>';
    }
  });
  list += '</ul></li>';
  return list;
}

function appendMenu(list){
  $(".major-menu").empty().append(list)
  bindClick();
}
