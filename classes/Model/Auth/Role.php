<?php
/**
 * Default auth role
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM\Model\Auth;

use Modseven\ORM\ORM;

class Role extends ORM
{

    /**
     * Relationships
     * @var array
     */
    protected array $_has_many = [
        'users' => ['model' => 'User', 'through' => 'roles_users'],
    ];

    /**
     * All rules
     * @return array
     */
    public function rules(): array
    {
        return [
            'name'        => [
                ['not_empty'],
                ['min_length', [':value', 4]],
                ['max_length', [':value', 32]],
            ],
            'description' => [
                ['max_length', [':value', 255]],
            ]
        ];
    }
}