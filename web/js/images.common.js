$(function() {

    $.wait = function(ms) {
        var defer = $.Deferred();
        setTimeout(function() { defer.resolve(); }, ms);
        return defer;
    };

    $.scroll_to = function(id, offset) {
        var
            element = $(id),
            offset = offset || 0;
        if (element.length) {
            $('html, body').animate({
                scrollTop: $(element).offset().top - offset,
            }, 300);
        }
    };

    $.monitor = function(url, caption, before, active, div, callback) {
        $(div)
            .html('<h2>' + caption + '</h2>')
            .append('<div class="common_progress_outer"><div class="common_progress_inner" id="progress"></div></div>')
            .append('<div class="common_error_message" id="menu_failure"></div>');

        var poll = function() {
            $.ajax({
                url: url,
                success: function(data) {
                    if (data.failed > 0) {
                        $('#menu_failure').html(data.failed + ' failed');
                    }
                    $('#progress').css('width', data.progress + '%');
                    if (data.status === 'acquired' || data.status === before || data.status === active) {
                        window.setTimeout(function() { poll(); }, 1000);
                    } else {
                        $(div).fadeOut(400)
                        $.wait(500).then(callback);
                    }
                },
                error: function(data) {
                },
            });
        };
        poll();
    };
});
