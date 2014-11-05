var granularityMs = 600000;

function cleanVal(val) {
    if (val == null || val == undefined) {
        return 0;
    }
    return val;
}

function dyDate(timestamp) {
    var date = new Date(timestamp);
    var hours = date.getHours();
    if (hours < 10) {
        hours = '0' + hours;
    }

    var minutes = date.getMinutes();
    if (minutes < 10) {
        minutes = '0' + minutes;
    }

    var niceDate =
        date.getFullYear() + "-" +
        (date.getMonth() + 1) + "-" +
        date.getDate() + " " +
        hours + ":" + minutes;

    return niceDate;
}

function getDayOfWeek(day) {
    if (day == 0) {
        return 'Sun';
    }
    else if (day == 1) {
        return 'Mon';
    }
    else if (day == 2) {
        return 'Tues';
    }
    else if (day == 3) {
        return 'Wed';
    }
    else if (day == 4) {
        return 'Thurs';
    }
    else if (day == 5) {
        return 'Fri';
    }
    else if (day == 6) {
        return 'Sat';
    }
}

function niceDates(timestamp) {
    var date = new Date(timestamp);
    var niceDate = getDayOfWeek(date.getDay()) + " " + dyDate(timestamp);
    return niceDate;
}

function getPartitionTimeBefore(timestamp) {
    return Math.floor(timestamp / granularityMs) * granularityMs;
}

function getPartitionTimeAfter(timestamp) {
    var item = (getPartitionTimeBefore(timestamp));
    if (item == timestamp) {
        return timestamp;
    }

    return item + granularityMs;
}

function getParamMapString(param) {
    var paramList = param.split('&');

    var paramMap = {};
    for (var i = 0; i < paramList.length; ++i) {
        var params = paramList[i].split('=');
        paramMap[params[0]] = params[1];
    }

    return paramMap;
}

function getParamMap() {
    var getParam = location.search.replace("?", "");
    return getParamMapString(getParam);
}

function rotate(matrix) {
    var width = matrix.length;
    if (width < 1)
        return [];
    var length = matrix[0].length;
    var rotated = [];
    for (var i = 0; i < length; i++) {
        var row = [];
        for (var j = 0; j < width; j++)
            row.push(matrix[j][i]);
        rotated.push(row);
    }
    return rotated;
}

function range() {
    var from = arguments[0];
    var to = arguments[1];
    var by = 1;
    if (arguments.length == 3)
        by = arguments[2];
    var vals = [];
    for (var i = from; i < to; i += by)
        vals.push(i);
    return vals;
}

function csv(matrix) {
    return matrix.map(function (x) {
        return x.join(",")
    }).join("\n")
}

function parseTopicData(data, topic) {
    if (!("frontend" in data))
        return {};
    var results = {};

    var headers = ["Date"];
    for (var tier in data)
        headers.push(tier);

    var times = data.frontend.times.map(dyDate);

    // count graph
    var counts = $.map(data, function (tier, name) {
        return [tier["counts"]]
    });
    results["counts"] = csv([headers].concat(rotate([times].concat(counts))));

    // completeness graph
    var producerCounts = data["frontend"]["counts"];
    var error = $.map(data, function (tier, name) {
        return [percentComplete(producerCounts, tier["counts"])]
    });
    results["completeness"] = csv([headers].concat(rotate([times].concat(error))));

    // lag graph
    var delays = $.map(data, function (tier, name) {
        return [tier["delay"]]
    });
    results["lag"] = csv([headers].concat(rotate([times].concat(delays))));

    return results;
}

function percentComplete(producer, tier) {
    result = [];
    for (var i = 0; i < producer.length; i++)
        result.push(100.0 * tier[i] / Math.max(producer[i], 0.0001));
    return result;
}
