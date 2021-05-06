package mockmr

var builtinViewReducers = map[string]string{
	"_count": `
function(key, values, rereduce) {
   if (rereduce) {
       var result = 0;
       for (var i = 0; i < values.length; i++) {
           result += values[i];
       }
       return result;
   } else {
       return values.length;
   }
}
`,
	"sum": `
function(key, values, rereduce) {
  var sum = 0;
  for(i=0; i < values.length; i++) {
    sum = sum + values[i];
  }
  return(sum);
}
`,
}
