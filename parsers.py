from datetime import datetime, timezone, timedelta


def timestamp_to_date(ts):
    if not ts:
        return ''
    return datetime.fromtimestamp(ts / 1000, timezone(timedelta(hours=-7))).strftime('%Y-%m-%d')


def parse_tax(tax_node):
    if not tax_node:
        return (
            '',
            '',
            ''
        )

    return (
        tax_node['taxName'] or '',
        tax_node['taxAmount']['currency'] or '',
        tax_node['taxAmount']['amount'] or ''
    )


def parse_tracking_info(tracking_node):
    if not tracking_node:
        return (
            '',
            '',
            '',
            '',
            '',
            '',
            ''
        )

    return (
        timestamp_to_date(tracking_node['shipDateTime']),                 # timestamp converted to date
        tracking_node['carrierName']['otherCarrier'] or '',
        tracking_node['carrierName']['carrier'] or '',
        tracking_node['methodCode'] or '',
        tracking_node['carrierMethodCode'] or '',
        tracking_node['trackingNumber'] or '',
        tracking_node['trackingURL'] or ''
    )


def parse_refund(order, order_line, refund_node):
    result = []
    if not refund_node:
        return []

    for refund_charge in refund_node['refundCharges']['refundCharge']:
        result.append((
            order['purchaseOrderId'],
            order['customerOrderId'],
            order_line['lineNumber'],
            order_line['item']['productName'],
            order_line['item']['sku'],
            refund_node['refundId'] or '',
            refund_node['refundComments'] or '',
            refund_charge['refundReason'] or '',
            refund_charge['charge']['chargeType'] or '',
            refund_charge['charge']['chargeName'] or '',
            refund_charge['charge']['chargeAmount']['currency'] or '',
            refund_charge['charge']['chargeAmount']['amount'] or '',
            *parse_tax(refund_charge['charge']['tax'])
        ))
    return result


def parse_walmart_order(order):
    order_general_data = []
    line_charges_data = []
    line_refund_data = []
    ship = order['shippingInfo']
    postal_address = ship['postalAddress']

    # orderLines
    for order_line in order['orderLines']['orderLine']:
        order_line_status = order_line['orderLineStatuses']['orderLineStatus'][0]
        order_line_fulfillment = order_line['fulfillment']

        order_general_data.append(
            (
                order['purchaseOrderId'],
                order['customerOrderId'] or '',
                order['customerEmailId'],
                timestamp_to_date(order['orderDate']),              # timestamp converted to date
                ship['phone'] or '',
                timestamp_to_date(ship['estimatedDeliveryDate']),   # timestamp converted to date
                timestamp_to_date(ship['estimatedShipDate']),       # timestamp converted to date
                ship['methodCode'] or '',
                postal_address['name'] or '',
                postal_address['address1'] or '',
                postal_address['address2'] or '',
                postal_address['city'] or '',
                postal_address['state'] or '',
                postal_address['postalCode'] or '',
                postal_address['country'] or '',
                postal_address['addressType'] or '',
                order_line['lineNumber'] or '',
                order_line['item']['productName'],
                order_line['item']['sku'],
                order_line['orderLineQuantity']['amount'],
                order_line['statusDate'],        # timestamp
                order_line_status['status'] or '',
                *parse_tracking_info(order_line_status['trackingInfo']),
                order_line_fulfillment['fulfillmentOption'] or '',
                order_line_fulfillment['shipMethod'] or '',
                order_line_fulfillment['storeId'] or '',
                timestamp_to_date(order_line_fulfillment['pickUpDateTime']),  # timestamp converted to date
                order_line_fulfillment['pickUpBy'] or '',
                order_line_fulfillment['shippingProgramType'] or '',
            )  # general data with products info
        )
        # refunds
        line_refund_data.extend(
            parse_refund(order, order_line, order_line['refund'])
        )

        # charge
        for charge in order_line['charges']['charge']:
            line_charges_data.append(
                (
                    order['purchaseOrderId'],
                    order['customerOrderId'],
                    order_line['lineNumber'],
                    order_line['item']['productName'],
                    order_line['item']['sku'],
                    charge['chargeType'] or '',
                    charge['chargeName'] or '',
                    charge['chargeAmount']['currency'] or '',
                    charge['chargeAmount']['amount'] or '',
                    *parse_tax(charge['tax'])
                )
            )

    return order_general_data, line_charges_data, line_refund_data
