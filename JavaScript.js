var stateText;
var cpuText;
var ramText;
var urlNumText;
var lastTen = [];
var queueSizeText;
var indexSizeText;
var errors = [];

$(document).ready(function () {
    AJAXCalls();
});

$("#reload").click(function () {
    stateText = "";
    cpuText = "";
    ramText = "";
    urlNumText = "";
    lastTen = [];
    queueSizeText = "";
    indexSizeText = "";
    errors = [];
    AJAXCalls();
});

$("#start").click(function () {
    $.ajax({
        type: "POST",
        url: "admin.asmx/startCrawling",
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        success: function (msg) {
            var data = eval(msg);
            console.log(data);
        },
        error: function (msg) {
            console.log("error");
            console.log(msg);
        }
    });
});

$("#stop").click(function () {
    $.ajax({
        type: "POST",
        url: "admin.asmx/stopCrawling",
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        success: function (msg) {
            var data = eval(msg);
            console.log(data);
        },
        error: function (msg) {
            console.log("error");
            console.log(msg);
        }
    });
});

$("#clear").click(function () {
    deleteAll();
});

$("#submitRetrieve").click(function () {
    var word = $("#retrieve").val();
    $("#result").empty();
    $.ajax({
        type: "POST",
        url: "admin.asmx/getPageTitle",
        data: JSON.stringify({
            "url": word
        }),
        contentType: "application/json; charset=utf-8",
        dataType: "json",
        success: function (msg) {
            var data = eval(msg);
            $("#result").append(data.d);
            console.log(data);
        },
        error: function (msg) {
            console.log("error");
            console.log(msg);
        }
    });
});

function deleteAll() {
    $.when(
        $.ajax({
            type: "POST",
            url: "admin.asmx/delete",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var data = eval(msg);
                console.log(data);
            },
            error: function (msg) {
                console.log("error");
                console.log(msg);
            }
        }),

        $.ajax({
            type: "POST",
            url: "admin.asmx/stopCrawling",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var data = eval(msg);
                console.log(data);
            },
            error: function (msg) {
                console.log("error");
                console.log(msg);
            }
        })
    ).then(function () {
        AJAXCalls();
    });
}

function AJAXCalls() {
    $.when(
        $.ajax({
            type: "POST",
            url: "admin.asmx/getState",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var data = eval(msg);
                console.log(data);
                stateText = data.d;
                console.log(stateText);
            },
            error: function (msg) {
                console.log("error");
                console.log(msg);
            }
        }),

        $.ajax({
            type: "POST",
            url: "admin.asmx/getCPU",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var data = eval(msg);
                console.log(data);
                cpuText = data.d + "%";
            },
            error: function (msg) {
                console.log("error");
                console.log(msg);
            }
        }),

        $.ajax({
            type: "POST",
            url: "admin.asmx/getRAM",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var data = eval(msg);
                console.log(data);
                ramText = data.d + "MB";
            },
            error: function (msg) {
                console.log("error");
                console.log(msg);
            }
        }),

        $.ajax({
            type: "POST",
            url: "admin.asmx/getLastTen",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var data = eval(msg);
                console.log(data);
                for (var key in data.d) {
                    lastTen.push(data.d[key]);
                }
            },
            error: function (msg) {
                console.log("error");
                console.log(msg);
            }
        }),

        $.ajax({
            type: "POST",
            url: "admin.asmx/getErrors",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var data = eval(msg);
                console.log(data);
                for (var key in data.d) {
                    errors.push(data.d[key]);
                }
            },
            error: function (msg) {
                console.log("error");
                console.log(msg);
            }
        }),

        $.ajax({
            type: "POST",
            url: "admin.asmx/getSizeOfQueue",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var data = eval(msg);
                console.log(data);
                queueSizeText = data.d + " pages";
            },
            error: function (msg) {
                console.log("error");
                console.log(msg);
            }
        }),

        $.ajax({
            type: "POST",
            url: "admin.asmx/getSizeOfIndex",
            contentType: "application/json; charset=utf-8",
            dataType: "json",
            success: function (msg) {
                var data = eval(msg);
                console.log(data);
                indexSizeText = data.d + " pages";
            },
            error: function (msg) {
                console.log("error");
                console.log(msg);
            }
        })
    ).then(function () {
        addToDashboard();
    });
}

function addToDashboard() {
    console.log("here");
    $("#state").empty();
    $("#cpu").empty();
    $("#ram").empty();
    $("#urlNum").empty();
    $("#lastTen").empty();
    $("#queueSize").empty();
    $("#indexSize").empty();
    $("#errors").empty();
    $("#state").append(stateText);
    $("#cpu").append(cpuText);
    $("#ram").append(ramText);
    for (var key in lastTen) {
        $("#lastTen").append("<li>" + lastTen[key] + "</li>");
    }
    for (var key in errors) {
        $("#errors").append("<li>" +errors[key] + "</li>");
    }
    $("#queueSize").append(queueSizeText);
    $("#indexSize").append(indexSizeText);
}