var avaliableTags = [];
var granularityMs = 600000;
var granularityMin = 10;
var offsetMs = granularityMs * 3;
var millisecInDay = 86400000;
var millisecInHour = 3600000;
var numPartitionsInDay = millisecInDay / granularityMs;

function createTopicGraphs(topic, startTime, endTime) {
    //$('#chartPanel').empty();

    // draw event counts and error rate graphs
    $.ajax({
        url: "./topics/" + topic + "/metrics",
        global: false,
        type: "GET",
        data: ( {
            'start': startTime,
            'end': endTime,
            'complete': '0.999'
        }),
        dataType: "text",
        async: true,
        success: function (data) {
            document.title = topic + " Load Statistics";
            drawGraphs(jQuery.parseJSON(data), topic, startTime, endTime);
        }
    });
}

function drawGraphs(data, topic, startTime, endTime) {
    var chartPanel = $("#chartPanel");
    var csvs = parseTopicData(data, topic);
    drawCountGraph(document.getElementById("countChart"), csvs["counts"], topic);
    drawErrorGraph(document.getElementById("errorChart"), csvs["completeness"], topic);
    drawLagGraph(document.getElementById("lagChart"), csvs["lag"], topic);

    // totals
    fillTotals(data);
}

function fillTotals(data) {
    var producerSum = data["frontend"].total;
    for (var tier in data) {
        var errorPercent = ((data[tier].total - producerSum) * 100) / producerSum;
        $("#tierCountTable").find('tbody').append($('<tr><td>' + tier + '</td>' +
            '<td>' + niceNumber(data[tier].total) + '</td>' +
            '<td>' + errorPercent.toPrecision(3) + '%</td></tr>'));
    }
}

function addChartDiv(id, parent, klass) {
    var div = document.createElement('div');
    $(div).addClass(klass);
    $(div).attr("id", id);
    parent.append(div);
    return div;
}

function drawLagGraph(lagDiv, csv, topic) {
    g = new Dygraph(
        // containing div
        lagDiv,
        csv,
        {
            showRoller: true,
            maxNumberWidth: 20,
            height: '850px',
            width: '300px',
            ylabel: 'Lag Time',
            labelsSeparateLines: true,
            title: topic + " Data Load Lag",
            yAxisLabelWidth: 70,
            yAxisLabelFormatter: niceTime,
            yValueFormatter: niceTime,
            legend: "always",
            hideOverlayOnMouseOut: false,
            showRoller: false,
            labelsDiv: 'lagChartLabels'
        }
    );
}

function drawCountGraph(countDiv, csv, topic) {
    g = new Dygraph(
        countDiv,
        csv,
        {
            showRoller: true,
            maxNumberWidth: 20,
            labelsDiv: 'countChartLabels',
            height: '850px',
            width: '300px',
            ylabel: 'Event Count',
            labelsSeparateLines: true,
            title: topic + " Event Counts",
            yAxisLabelWidth: 50,
            yAxisLabelFormatter: truncNumber,
            yValueFormatter: niceNumber,
            legend: "always",
            hideOverlayOnMouseOut: false,
            showRoller: false
        }
    );
}

function drawErrorGraph(errorDiv, csv, topic) {
    g = new Dygraph(
        // containing div
        errorDiv,
        csv,
        {
            maxNumberWidth: 20,
            height: '10px',
            width: '300px',
            ylabel: '% Complete',
            errorBars: false,
            fillAlpha: 0.5,
            labelsSeparateLines: true,
            avoidMinZero: true,
            title: topic + " Completeness Percentage",
            legend: "always",
            hideOverlayOnMouseOut: false,
            showRoller: false,
            labelsDiv: 'errorChartLabels'
        }
    );
}

function niceNumber(number) {
    var numberText = "" + number;
    var finalNum = "";

    while (numberText.length > 3) {
        var length = numberText.length;
        var substr = numberText.substring(length - 3, length);

        finalNum = "," + substr + finalNum;
        numberText = numberText.substring(0, length - 3);
    }

    finalNum = numberText + finalNum;

    return finalNum;
}

function truncNumber(x) {
    if (x > 1000000000)
        return x / 1000000000 + "B";
    if (x > 1000000)
        return x / 1000000 + "M";
    else if (x > 1000)
        return x / 1000 + "K";
    else
        return x;
}

function niceTime(x) {
    if (x < 1000)
        return x + " ms";
    else if (x < 5 * 60 * 1000)
        return (x / 1000).toFixed(0) + " secs";
    else
        return (x / (60 * 1000)).toFixed(0) + " mins";
}

function getThreeDigitStr(number) {
    var modNum = (number % 1000);
    var textNum = "" + modNum;
    if (modNum < 10) {
        textNum = "00" + textNum;
    }
    else if (modNum < 100) {
        textNum = "0" + textNum;
    }

    return textNum;
}

function getQueryParams() {
    queryTopic = $('#topicList').val();
    if (queryTopic == undefined || queryTopic == null) {
        alert("Topic can't be empty.");
        return;
    }

    var fromHour = $("#fromHour").val();
    if (isNaN(fromHour)) {
        alert("From hour must be a value between 0-23");
    }

    var toHour = $("#toHour").val();
    if (isNaN(toHour)) {
        alert("To hour to must be a value between 0-23");
    }

    var fromMin = $("#fromMin").val();
    if (isNaN(fromMin)) {
        alert("From min to must be a value between 0-59");
    }

    var toMin = $("#toMin").val();
    if (isNaN(toMin)) {
        alert("To min to must be a value between 0-59");
    }

    var beginDate = $("#startPicker").datepicker("getDate");
    beginDate.setHours(fromHour);
    beginDate.setMinutes(fromMin);

    var endDate = $("#endPicker").datepicker("getDate");
    endDate.setHours(toHour);
    endDate.setMinutes(toMin);

    var beginTimestamp = beginDate.getTime();
    var endTimestamp = endDate.getTime();
    if (beginTimestamp > endTimestamp) {
        alert("The 'to' date must occur after the 'from' date.")
    }

    beginTimestamp = getPartitionTimeBefore(beginTimestamp);
    var date = new Date(beginTimestamp);
    $("#fromHour").val(date.getHours());
    $("#fromMin").val(date.getMinutes());

    endTimestamp = getPartitionTimeAfter(endTimestamp);
    date = new Date(endTimestamp);
    $("#toHour").val(date.getHours());
    $("#toMin").val(date.getMinutes());

    $.cookie('monitorTool', queryTopic);

    var returnVal = {};
    returnVal.topic = queryTopic;
    returnVal.beginTimestamp = beginTimestamp;
    returnVal.endTimestamp = endTimestamp;

    return returnVal;
}

function createOption(value) {
    var option = document.createElement("option");
    $(option).attr("value", "00");

    return option;
}

function fillTopic(topics, defaultVal) {
    for (var i = 0; i < topics.length; ++i) {
        option = document.createElement("option");
        $(option).val(topics[i]);
        $(option).text(topics[i]);

        $('#topicList').append(option);
        if (defaultVal == topics[i]) {
            $(option).attr('selected', 'selected');
        }
    }
}

function togglePicker() {
    if ($("#rangeRadio").attr("checked") == 'checked') {
        $("#past :input").attr('disabled', true);
        $("#range :input").removeAttr('disabled');
    }
    else if ($("#pastRadio").attr("checked") == 'checked') {
        $("#range :input").attr('disabled', true);
        $("#past :input").removeAttr('disabled');
    }
}

var queryTopic;
$(function () {
    var getParam = getParamMap();

    var shouldAutoLoad = false;
    var isSpecificRange = false;
    var defaultVal = $.cookie('monitorTool');
    if (getParam.topic != null) {
        defaultVal = getParam.topic;
        shouldAutoLoad = true;
    }

    var latestHours = 24;
    if (getParam.lastHours != null) {
        latestHours = parseInt(getParam.lastHours);

        if (getParam.offset != null) {
            offsetMs = getParam.offset;
        }
    }
    $("#lastHours").val(latestHours);
    if (offsetMs != 0) {
        var numMin = offsetMs / 60000;
        $("#hoursText").text("hours, " + numMin + " min delay");
    }

    var date = new Date();
    var partitionTime = getPartitionTimeAfter(date.getTime());
    var fromDate = new Date(partitionTime - millisecInDay);
    var toDate = new Date(partitionTime);
    if (getParam.beginTimestamp != null) {
        fromDate = new Date(parseInt(getParam.beginTimestamp));
        isSpecificRange = true;
    }
    if (getParam.endTimestamp != null) {
        toDate = new Date(parseInt(getParam.endTimestamp));
        isSpecificRange = true;
    }

    $("#fromHour").val(fromDate.getHours());
    $("#fromMin").val(fromDate.getMinutes());
    $("#toHour").val(toDate.getHours());
    $("#toMin").val(toDate.getMinutes());

    $("#startPicker").datepicker();
    $("#endPicker").datepicker();
    $("#startPicker").datepicker("setDate", fromDate);
    $("#endPicker").datepicker("setDate", toDate);

    $("#rangeRadio").click(function () {
        togglePicker();
    });
    $("#pastRadio").click(function () {
        togglePicker();
    });

    if (isSpecificRange) {
        $("#rangeRadio").attr("checked", 'checked');
    }
    else {
        $("#pastRadio").attr("checked", 'checked');
    }
    togglePicker();

    $.ajax({
        url: "./topics",
        global: false,
        type: "GET",
        dataType: "json",
        async: true,
        success: function (data) {
            fillTopic(data, defaultVal);

            if (shouldAutoLoad) {
                $('#topicList').val(defaultVal);
                var params = getQueryParams();
                if ($("#rangeRadio").attr("checked")) {
                    createTopicGraphs(params.topic, params.beginTimestamp, params.endTimestamp);
                }
                else if ($("#pastRadio").attr("checked")) {
                    var date = new Date();
                    var partitionTime = getPartitionTimeAfter(date.getTime() - offsetMs);

                    createTopicGraphs(params.topic, partitionTime - millisecInHour * (parseInt($("#lastHours").val())), partitionTime);
                }
            }
        }
    });

    $("#queryButton").click(function () {
        var options = getQueryParams();
        var redirectLoc;
        if ($("#rangeRadio").attr("checked") == 'checked') {
            redirectLoc = "?topic=" + options.topic + "&beginTimestamp=" + options.beginTimestamp + "&endTimestamp=" + options.endTimestamp;
        }
        else {
            redirectLoc = "?topic=" + options.topic + "&lastHours=" + $("#lastHours").val();
        }
        window.location.href = redirectLoc;
    });
});
