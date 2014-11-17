var queryTopic;
var numHours = 12;
var defaultLinkHours = 24;
var millisecInHour = 3600000;
var toTime; // endtime, to be filled later
var fromTime;
var offsetMs = 600000 * 3;

function drawGraphCellPanel(topic, div, data) {
    g = new Dygraph(
        // containing div
        div,
        data,
        {
            maxNumberWidth: 20,
            height: 200,
            width: 350,
            ylabel: '% Complete',
            errorBars: false,
            fillAlpha: 0.5,
            labelsSeparateLines: true,
            avoidMinZero: true,
            title: topic,
            legend: "always",
            hideOverlayOnMouseOut: false,
            showRoller: false
        }
    );

    $(div).click(function () {
        var redirectLoc = "topicgraph.html?topic=" + topic + "&lastHours=" + defaultLinkHours;
        window.location.href = redirectLoc;
    });
}

function drawEmptyCellPanel(topic) {
    var errorLabelDiv = document.createElement('div');
    $(errorLabelDiv).addClass('noResultLabel');
    $(errorLabelDiv).text(topic + "\nhas no results for the past 12 hours");
    $("#" + topic + "-cell").append(errorLabelDiv);
}

var numColumns = 3;
function createTopicCells(topics) {
    //topics.length / numColumns + 1;
    var tr;
    for (var i = 0; i < topics.length; ++i) {
        var topic = topics[i];
        if ((i % numColumns) == 0) {
            tr = document.createElement("tr");
            $("#graphTable").append(tr);
        }

        var td = document.createElement("td");
        $(td).attr("id", topic + "-cell");
        $(td).addClass("graphCell");
        $(tr).append(td);

        var errorDiv = document.createElement('div');
        $(errorDiv).addClass('smallChart');
        var id = topic + '-chart';
        $(errorDiv).attr("id", id);
        $(td).append(errorDiv);

        $.ajax({
            url: "./topics/" + topic + "/metrics",
            global: false,
            type: "GET",
            data: ( {
                'start': fromTime,
                'end': toTime
            }),
            dataType: "json",
            async: true,
            success: function (data) {
                var topicExp = /.*\/topics\/([^/]*)+.*/g;
                var topic = topicExp.exec(this.url)[1];
                var result = parseTopicData(data, topic);
                if ("completeness" in result) {
                    drawGraphCellPanel(topic, document.getElementById(topic + '-chart'), result['completeness']);
                } else {
                    drawEmptyCellPanel(topic);
                }
            }
        });
    }
}

$(function () {
    var getParam = getParamMap();

    var date = new Date();
    toTime = getPartitionTimeBefore(date.getTime() - offsetMs);
    fromTime = toTime - 12 * millisecInHour;

    var timeHeader = niceDates(fromTime) + " to " + niceDates(toTime);
    if (getParam.offset != null) {
        offsetMs = getParam.offset;
    }
    if (offsetMs != null) {
        var numMin = offsetMs / 60000;
        timeHeader += ", " + numMin + " min delay";
    }

    $("#timeHeader").text(timeHeader);

    $.ajax({
        url: "./topics",
        global: false,
        type: "POST",
        dataType: "json",
        async: true,
        success: function (data) {
            createTopicCells(data);
        }
    });

});