$(function() {
    var autosave = function (id, validator, preprocessor) {
        $(id).change(function () {
            $(id).removeClass('autosave_error').addClass('autosave_saving');
            var url = $(id)[0].getAttribute('data-url');
            var field = $(id)[0].getAttribute('data-name');
            var data = new Object();
            var value =  $(id).val();
            if (validator !== undefined) {
                if (!validator(value)) {
                    $(id).removeClass('autosave_saving').addClass('autosave_error');
                    return;
                }
            }
            if (preprocessor !== undefined) {
                value = preprocessor(value);
            }
            data[field] = value;
            $.ajax({
                url: url,
                method: 'PATCH',
                contentType: "application/json",
                data: JSON.stringify(data),
                success: function (data) {
                    $(id).removeClass('autosave_saving');
                },
                error: function (data) {
                    $(id).removeClass('autosave_saving').addClass('autosave_error');
                },
            });
        })
    };

    $.Images = $.Images || {};
    $.Images.autosave = autosave;
});
