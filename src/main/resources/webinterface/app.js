function transform() {
	console.log("transform()");
	var frm = document.frm;
	frm.action = "/transform";
	frm.method = "POST";
	frm.submit();

	return false;
}

function execute(level, option) {
	var frm = document.frm;
	var params = "level=" + level + "&option=" + option;
	frm.action = "/execute?" + params;
	frm.method = "GET";
	frm.submit();

	console.log("execute() params: " + params);
	
	return false;
}

function showgraph() {
		window.open("http://localhost:7474/browser/", "_blank");
}

function setPreset(set) {
	console.log("setPreset set: " + set);

	switch(set) {
		case "basic":
			data = preset.basic;
			break;
		case "zoom1":
			data = preset.zoom1;
			break;
	}
	frm.constraint.value = data.constraint;
	frm.rule.value = data.rule;
}

