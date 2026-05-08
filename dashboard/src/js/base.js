import moment from 'moment-timezone';

const LOCALSTORAGE_AUTOLOAD_KEY = 'horizonAutoLoadsNewEntries';

export default {
    computed: {
        Horizon() {
            return Horizon;
        },
    },

    methods: {
        /**
         * Format the given date with respect to timezone.
         */
        formatDate(unixTime) {
            return moment(unixTime * 1000).add(new Date().getTimezoneOffset() / 60);
        },

        /**
         * Format the given date with respect to timezone.
         */
        formatDateIso(date) {
            return moment(date).add(new Date().getTimezoneOffset() / 60);
        },

        /**
         * Extract the job base name.
         */
        jobBaseName(name) {
            if (!name) {
                return name;
            }

            return name.split(/[\\\\:.]/).pop();
        },

        /**
         * Autoload new entries in listing screens.
         */
        autoLoadNewEntries() {
            this.autoLoadsNewEntries = !this.autoLoadsNewEntries;
            localStorage[LOCALSTORAGE_AUTOLOAD_KEY] = Number(this.autoLoadsNewEntries);
        },

        /**
         * Convert to human readable timestamp.
         */
        readableTimestamp(timestamp) {
            return this.formatDate(timestamp).format('YYYY-MM-DD HH:mm:ss');
        },

        /**
         * Uppercase the first character of the string.
         */
        upperFirst(string) {
            return string.charAt(0).toUpperCase() + string.slice(1);
        },

        /**
         * Group array entries by a given key.
         */
        groupBy(array, key) {
            return array.reduce(
                (grouped, entry) => ({
                    ...grouped,
                    [entry[key]]: [...(grouped[entry[key]] || []), entry],
                }),
                {}
            );
        },

        /**
         * Show a modal alert using the shared dashboard component.
         */
        showAlert(type, message, autoClose = 0) {
            this.$root.alert.type = type;
            this.$root.alert.message = message;
            this.$root.alert.autoClose = autoClose;
        },

        /**
         * Show an error alert using the shared dashboard component.
         */
        showErrorAlert(message) {
            this.showAlert('error', message);
        },

        /**
         * Extract a readable error message from an HTTP client error.
         */
        extractErrorMessage(error, fallback = 'Something went wrong.') {
            const detail = error?.response?.data?.detail;

            if (typeof detail === 'string' && detail.trim()) {
                return detail;
            }

            if (detail && typeof detail === 'object') {
                if (typeof detail.message === 'string' && detail.message.trim()) {
                    return detail.message;
                }
            }

            const message = error?.response?.data?.error;

            if (typeof message === 'string' && message.trim()) {
                return message;
            }

            return fallback;
        },
    },
};
