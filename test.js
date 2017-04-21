
const queue = require('./lib/squeue')('mongodb://localhost/carved-wholesale');

const data = { event: 'ADDED',
  data:
   { __v: 0,
     updated_at: 'Fri Apr 21 2017 14:57:39 GMT-0400 (EDT)',
     created_at: 'Fri Apr 21 2017 14:57:39 GMT-0400 (EDT)',
     po_number: 'BRN-00006',
     status: '589e41336caf1b09faa701e7',
     customer: '58d91da4ea54fd4445e35bfb',
     purchaser: 'Bryan',
     order_date: 'Fri Apr 21 2017 14:56:59 GMT-0400 (EDT)',
     due_date: 'Mon May 01 2017 14:56:57 GMT-0400 (EDT)',
     payment_terms: 'Net30',
     instructions: '',
     invoice_number: 'CV-03006',
     tracking_number: '',
     shipping_account: '',
     shipping_method: '',
     billing_address:
      { name: 'Bryan',
        address_one: '123454 Some Street',
        address_two: '',
        city: 'Elkhart',
        state: 'IN',
        zip: '12345',
        country: 'US',
        _id: '58d91da4ea54fd4445e35bfd' },
     shipping_address:
      { name: 'Bryan',
        address_one: '123454 Some Street',
        address_two: '',
        city: 'Elkhart',
        state: 'IN',
        zip: '12345',
        country: 'US',
        _id: '58d91da4ea54fd4445e35bfd' },
     _id: '58fa562382c04122dfebd825',
     is_deleted: false,
     files: [],
     products: [ ],
     invoice_note: '',
     completer: '',
     weight: '',
     dimensions: '',
     same_shipping: true,
     grand_total: 50,
     sub_total: 50,
     discount_amount: null,
     shipping_amount: null,
     tax_amount: null,
     invoice_sent: false,
     id: '58fa562382c04122dfebd825' } };


queue
  .add(data)
  .then(() => {
    console.log('done');
    process.exit();
  })
  .catch(error => {
    console.log(error);
    process.exit(1);
  });
