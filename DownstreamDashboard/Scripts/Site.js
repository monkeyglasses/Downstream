$(function () {
    // Hide menus when clicking outside of them
    $('html').on('click', function () {
        $('#search-widget ul').fadeOut(200);
    });

    $('#prod-switch button').on('click', function (e) {
        var button = $(this);
        var contentHeader = $('#main-section');
        var label = $('#main-section #content-header h2 span');
        contentHeader.fadeOut({
            complete: function () {
                label.html(button.html());
                $(this).fadeIn();
            }
        });
        button.toggleClass('active').siblings().toggleClass('active');
    });

    $('#status-switches .status a').on('click', function (e) {
        var toggle = $(this);
        var parentVal = toggle.parents('.status').attr('data');
        var parentValId = toggle.parents('.status').attr('id');
        var typeVal = toggle.parent().attr('data');

        var newGridById = $('#details-grid-' + parentValId + '-extractions');
        var grid = $('#details-grid');
        var gridValue = grid.children('h3');

        var currentActiveGrid = $('#details-grid').find('.active')[0];
        $('#status-switches a').removeClass('active');
        $('#details-grid-' + parentValId + '-extractions').removeClass('active'); // remove active class from all datagrids

        toggle.addClass('active');

        $(currentActiveGrid).fadeOut({
            complete: function () {
                gridValue.html(parentVal + " - " + typeVal);
                $('#details-grid-completed-extractions').addClass('hidden'); // add hidden class to all datagrids
                $('#details-grid-errored-extractions').addClass('hidden'); // add hidden class to all datagrids
                $('#details-grid-in-progress-extractions').addClass('hidden'); // add hidden class to all datagrids
                $(newGridById).addClass('active');
                $(newGridById).removeClass('hidden');
                $(newGridById).fadeIn();
            }
        });

        return false;
    });

    // Show hide details on extractions list
    $('#extractions-list li').on('click', function (e) {
        var item = $(this);
        if (item.attr('data') != 'notStarted') {
            item.children('div').slideToggle(100);
            item.toggleClass('opened');
        }
    });

    // Filtering
    $('select.filter').on('change', function (e) {
        var speed = 200;
        selected = $(this).find('option:selected').attr('data');
        extractionsList = $("#extractions-list");
        if (selected == 'all') {
            extractionsList.find('.extraction-row').slideDown(speed);
        } else {
            extractionsList.find(".extraction-row").slideUp(speed);
            extractionsList.find(".extraction-row[data=" + selected + "]").slideDown(speed);
        }
    });

    // Dropdown Menu
    $('li.dropdown, li.dropdown a[data-toggle]=dropdown-menu').on('click', function () {
        $('.dropdown-menu').fadeToggle(200);
        $('#search-widget ul').fadeOut(200);
        return false;
    });


    // Search Stuff
    $('#search-widget input').on('keyup', function () {
        var letter = $(this).val();
        if (letter == 'a' || letter == 'A') {
            $(this).siblings('ul.no-results').hide();
            $(this).siblings('ul.results').fadeIn(200);
        } else {
            $(this).siblings('ul.results').hide();
            $(this).siblings('ul.no-results').fadeIn(200);
        }
    });

    $('#search-widget ul.results li').on('click', function () {
        var selection = $(this);
        var data = selection.attr('data');
        selection.parent().fadeOut(200)
        $('#result').fadeOut(100, function () {
            $('#result h2.screen-title').html("Extraction Status For " + data);
            $(this).fadeIn(100);
        });
    });

    $('#result a.close-icon').on('click', function () {
        $(this).parent().fadeOut(200);
    });

    $('#notifications a.close-icon-dark').on('click', function () {
        $(this).parent().fadeOut(200);
    });
    // End Search Stuff


    // Setup Timer
    setupTimer();

    // Load Graph
    d3Graph1();
    d3Graph2();

});

function setupTimer() {
    var start = moment();
    var end = moment().add('h', 3).add('m', 45).add('s', 32);
    var duration = moment.duration({ hours: 3, minutes: 47, seconds: 45 });
    var cdHours = $('#countdown .hours');
    var cdMins = $('#countdown .minutes');
    var cdSecs = $('#countdown .seconds');
    setInterval(function () {
        duration = duration.subtract(1, 's');
        cdHours.html("0" + duration.hours() + ":");
        cdMins.html(duration.minutes());
        cdSecs.html(duration.seconds());
    }, 1000);

}

function d3Graph1() {
    // define dimensions of graph
    var m = [70, 80, 40, 120]; // margins
    var w = 850 - m[1] - m[3];	// width
    var h = 350 - m[0] - m[2]; // height

    // create a simple data array that we'll plot with a line (this array represents only the Y values, X will just be the index location)
    var data1 = [3, 6, 2, 7, 5, 2, 0, 3, 8, 9, 2, 5, 9, 3, 6, 3, 6, 2, 7, 5, 2, 1, 3, 8, 9, 2, 5, 9, 2, 7];
    var data2 = [543, 367, 215, 56, 65, 62, 87, 156, 287, 398, 523, 685, 652, 674, 639, 619, 589, 558, 605, 574, 564, 496, 525, 476, 432, 458, 421, 387, 375, 368];

    // X scale will fit all values from data[] within pixels 0-w
    var x = d3.scale.linear().domain([0, data1.length]).range([0, w]);
    // Y scale will fit values from 0-10 within pixels h-0 (Note the inverted domain for the y-scale: bigger is up!)
    var y1 = d3.scale.linear().domain([0, 10]).range([h, 0]); // in real world the domain would be dynamically calculated from the data
    var y2 = d3.scale.linear().domain([0, 700]).range([h, 0]);  // in real world the domain would be dynamically calculated from the data
    // automatically determining max range can work something like this
    // var y = d3.scale.linear().domain([0, d3.max(data)]).range([h, 0]);

    // create a line function that can convert data[] into x and y points
    var line1 = d3.svg.line()
        // assign the X function to plot our line as we wish
        .x(function (d, i) {
            // return the X coordinate where we want to plot this datapoint
            return x(i);
        })
    .y(function (d) {
        // return the Y coordinate where we want to plot this datapoint
        return y1(d);
    })

    // create a line function that can convert data[] into x and y points
    var line2 = d3.svg.line()
        // assign the X function to plot our line as we wish
        .x(function (d, i) {
            // return the X coordinate where we want to plot this datapoint
            return x(i);
        })
    .y(function (d) {
        // return the Y coordinate where we want to plot this datapoint
        return y2(d);
    })


    // Add an SVG element with the desired dimensions and margin.
    var graph = d3.select("#graph").append("svg:svg")
        .attr("width", w + m[1] + m[3])
        .attr("height", h + m[0] + m[2])
        .append("svg:g")
        .attr("transform", "translate(" + m[3] + "," + m[0] + ")");

    // create yAxis
    var xAxis = d3.svg.axis().scale(x).tickSize(-h).tickSubdivide(true);
    // Add the x-axis.
    graph.append("svg:g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + h + ")")
        .call(xAxis);


    // create left yAxis
    var yAxisLeft = d3.svg.axis().scale(y1).ticks(4).orient("left");
    // Add the y-axis to the left
    graph.append("svg:g")
        .attr("class", "y axis axisLeft")
        .attr("transform", "translate(-15,0)")
        .call(yAxisLeft);

    // create right yAxis
    var yAxisRight = d3.svg.axis().scale(y2).ticks(6).orient("right");
    // Add the y-axis to the right
    graph.append("svg:g")
        .attr("class", "y axis axisRight")
        .attr("transform", "translate(" + (w + 15) + ",0)")
        .call(yAxisRight);

    // add lines
    // do this AFTER the axes above so that the line is above the tick-lines
    graph.append("svg:path").attr("d", line1(data1)).attr("class", "data1");
    graph.append("svg:path").attr("d", line2(data2)).attr("class", "data2");


}

function d3Graph2() {
    var margin = { top: 30, right: 20, bottom: 30, left: 100 },
        width = 850 - margin.left - margin.right,
        height = 350 - margin.top - margin.bottom;

    var formatPercent = d3.format("");

    var x = d3.scale.ordinal()
        .rangeRoundBands([0, width], 0.1);

    var y = d3.scale.linear()
        .range([height, 0]);

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom");

    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left")
        .tickFormat(formatPercent);

    var svg = d3.select("#graph2").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    d3.tsv("../../data/data.tsv", type, function (error, data) {
        x.domain(data.map(function (d) { return d.letter; }));
        y.domain([0, d3.max(data, function (d) { return d.frequency; })]);

        svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

        svg.append("g")
            .attr("class", "y axis")
            .call(yAxis)
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", 6)
            .attr("dy", ".71em")
            .style("text-anchor", "end")
            .text("Average Time");

        svg.selectAll(".bar")
            .data(data)
            .enter().append("rect")
            .attr("class", "bar")
            .attr("x", function (d) { return x(d.letter); })
            .attr("width", x.rangeBand())
            .attr("y", function (d) { return y(d.frequency); })
            .attr("height", function (d) { return height - y(d.frequency); });

    });

    function type(d) {
        d.frequency = +d.frequency;
        return d;
    }

}
